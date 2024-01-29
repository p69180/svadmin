import subprocess
import re
import json
import multiprocessing
import socket

import pandas as pd

import utils


PATSTR_BASE = r'{}\s*:\s*(?P<r>(?P<r_val>\S+) (?P<r_unit>\S+))\s*\|\s*{}\s*:\s*(?P<w>(?P<w_val>\S+) (?P<w_unit>\S+))\s*'
PAT_LINE1 = re.compile(PATSTR_BASE.format('Total DISK READ', 'Total DISK WRITE'))
PAT_LINE2 = re.compile(PATSTR_BASE.format('Actual DISK READ', 'Actual DISK WRITE'))

EXPONENTS = {'B': 0, 'K': 1, 'M': 2, 'G': 3, 'T': 4}


def postprocess(string):
    # string: 55.36 K/s
    stringsp = string.split()
    value = float(stringsp[0])
    unit_numer, unit_denom = stringsp[1].split('/')
    assert unit_denom == 's'

    factor = 1024 ** EXPONENTS[unit_numer]
    value_bytes = value * factor
    values_mb = value_bytes / 1024**2
    return values_mb


def run_iotop():
    p = subprocess.run(['iotop', '-b', '-n', '1'], text=True, capture_output=True)
    split_stdout = p.stdout.strip().split('\n')
    mat_line1 = PAT_LINE1.fullmatch(split_stdout[0])
    mat_line2 = PAT_LINE2.fullmatch(split_stdout[1])
    assert (mat_line1 is not None) and (mat_line2 is not None)

    # these values are all MB/sec
    return {
        'total_read(MB/s)': postprocess(mat_line1.group('r')),
        'total_write(MB/s)': postprocess(mat_line1.group('w')),
        'actual_read(MB/s)': postprocess(mat_line2.group('r')),
        'actual_write(MB/s)': postprocess(mat_line2.group('w')),
        'hostname': socket.gethostname(),
    }


def main():
    utils.rootcheck()

    # run iotop over ssh
    with multiprocessing.Pool() as pool:
        argsiter = ((x, run_iotop) for x in utils.ALL_NODES)
        #node_results = pool.starmap(utils.run_over_ssh, argsiter)
        node_results = dict(zip(utils.ALL_NODES, pool.starmap(utils.run_over_ssh, argsiter)))

    # make result dataframe
    df_records = list()
    for key, val in node_results.items():
        if val is None:
            df_records.append(
                {
                    'total_read(MB/s)': pd.NA,
                    'total_write(MB/s)': pd.NA,
                    'actual_read(MB/s)': pd.NA,
                    'actual_write(MB/s)': pd.NA,
                    'hostname': key,
                } 
            )
        else:
            df_records.append(val)

    df = pd.DataFrame.from_records(df_records)
    df = df.set_index('hostname').loc[utils.ALL_NODES, :]
    sumrow = df.sum(axis=0)
    sumrow.name = 'SUM'
    df = pd.concat([df, sumrow.to_frame().T], axis=0)

    print(df)
    
    
if __name__ == '__main__':
    main()
