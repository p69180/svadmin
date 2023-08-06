import os
import subprocess
import pprint
import collections
import itertools
import time

import psutil
import numpy as np
import pandas as pd


def init_tgroup(linedict):
    tgroup = dict()
    tgroup['all_items'] = list()
    tgroup['threads'] = list()
    tgroup['leader'] = linedict
    return tgroup


def finalize_tgroup(tgroup):
    assert len(set(x['USER'] for x in tgroup['all_items'])) == 1
    tgroup['user'] = tgroup['leader']['USER']
    tgroup['pid'] = tgroup['leader']['PID']
    tgroup['cmd'] = tgroup['leader']['CMD']
    #tgroup['pmem'] = tgroup['leader']['%MEM']
    tgroup['rss_kb'] = tgroup['leader']['RSS']


def ps_linedict_sanitycheck(linedict):
    if linedict['PID'] is None:  # threads
        assert all(
            (linedict[key] is None)
            for key in ('CMD', 'RSS')
        ), f'{linedict}'
    else:  # tgroup leader process
        assert all(
            (linedict[key] is None)
            for key in ('S', 'TID')
        ), f'{linedict}'


def make_ps_linedict(line, columns):
    # make linesp
    tmp_linesp = line.split()
    if len(tmp_linesp) != len(columns):
        assert len(tmp_linesp) > len(columns)
        cmd = tmp_linesp[(len(columns) - 1):]
    else:
        cmd = [tmp_linesp[-1]]

    if cmd == ['-']:
        cmd = '-'

    linesp = tmp_linesp[:(len(columns) - 1)] + [cmd]

    # make linedict 
    linedict = dict(
        zip(
            columns, 
            ((None if x == '-' else x) for x in linesp),
        )
    )

    # 
    #for key in ['%CPU']:
    #    linedict[key] = float(linedict[key])
    #for key in ['PID', 'RSS', 'TID']:
    #    linedict[key] = int(linedict[key])

    return linedict


def ps_output_iterator(stdout_split, columns):
    # CMD must come at the end
    if 'CMD' in columns:
        assert columns.index('CMD') == len(columns) - 1

    init = True
    for line in stdout_split[1:]:
        linedict = make_ps_linedict(line, columns)

        if linedict['PID'] is not None:
            if init:
                init = False
            else:
                finalize_tgroup(tgroup)
                yield tgroup
            tgroup = init_tgroup(linedict)

        ps_linedict_sanitycheck(linedict)
        tgroup['all_items'].append(linedict)
        if linedict['PID'] is None:
            tgroup['threads'].append(linedict)


def run_ps():
    p = subprocess.run(
        ['ps', '-mA', '-o', 'pid,tid,state,user:50,pcpu,rss,cmd'],
        text=True,
        capture_output=True,
    )
    stdout_split = p.stdout.strip().split('\n')
    columns = stdout_split[0].split()
    tgroup_list = list(ps_output_iterator(stdout_split, columns))

    return tgroup_list


########

def df_sanitycheck(df):
    assert 'pid' in df.columns
    assert df.loc[df['pid'].isna(), ['cmd', 'rss']].isna().all(axis=None)
    assert df.loc[df['pid'].notna(), ['state', 'tid']].isna().all(axis=None)


def subdf_sanitycheck(subdf):
    assert pd.notna(subdf['pid'].iloc[0])
    assert subdf['pid'].iloc[1:].isna().all(), str(subdf)
    assert len(subdf['user'].unique()) == 1


def merge_subdf(subdf):
    result = dict()

    result['num_R'] = (subdf['state'] == 'R').sum()
    result['num_D'] = (subdf['state'] == 'D').sum()
    result['num_S'] = (subdf['state'] == 'S').sum()

    second_to_end = subdf.iloc[1:, :]
    result['pcpu_sum'] = second_to_end['pcpu'].sum()
    result['num_threads'] = second_to_end.shape[0]

    firstrow = subdf.iloc[0, :]
    result['pid'] = firstrow['pid']
    result['user'] = firstrow['user']
    result['rss_kb'] = firstrow['rss']
    result['cmd'] = firstrow['cmd']

    return result


def postprocess_df(df):
    df_sanitycheck(df)

    # make grouper
    pids = list()
    na_lengths = list()
    for is_na, subiter in itertools.groupby(
        zip(df['pid'].isna(), df['pid']),
        key=(lambda x: x[0]),
    ):
        subiter_tup = tuple(subiter)
        if is_na:
            na_lengths.append(len(subiter_tup))
        else:
            assert len(subiter_tup) == 1
            pids.append(subiter_tup[0][1])
    grouper = np.repeat(pids, np.asarray(na_lengths) + 1)

    # do groupby on df
    tgroups = list()
    tmerge_df_data = list()
    for key, subdf in df.groupby(grouper):
        subdf_sanitycheck(subdf)
        # tgroups
        groupspec = dict()
        groupspec['pid'] = key
        groupspec['all_lines'] = subdf
        groupspec['leader'] = subdf.iloc[0, :]  # a Series
        groupspec['threads'] = subdf.iloc[1:, :]  # a DataFrame
        tgroups.append(groupspec)
        # tmerge_df_data
        tmerge_df_data.append(merge_subdf(subdf))
    tmerge_df = pd.DataFrame.from_records(tmerge_df_data)

    return tgroups, tmerge_df


def run_ps_read_with_pandas(ps_format, int_keys, float_keys):
    # prepare ps arguments
    o_arg = list()
    for x in ps_format:
        if x[1] is None:
            o_arg.append(f'{x[0]}={x[0]}')
        else:
            o_arg.append(f'{x[0]}:{x[1]}={x[0]}')
    ps_args = ['ps', '-mA', '-o', ','.join(o_arg)]

    # prepare pandas parameters
    header = [x[0] for x in ps_format]
    if 'cmd' in header:
        assert header.index('cmd') == len(header) - 1
    dtype = collections.defaultdict(pd.StringDtype)
    for key in int_keys:
        if key in header:
            dtype[key] = pd.Int64Dtype()
    for key in float_keys:
        if key in header:
            #dtype[key] = float
            dtype[key] = pd.Float64Dtype()

    def bad_line_handler(bad_line):
        assert len(bad_line) > len(header)
        new_line = list()
        new_line.extend(bad_line[:(len(header) - 1)])
        new_line.append(' '.join(bad_line[(len(header) - 1):]))
        return new_line
            
    # run ps and read with pandas
    p = subprocess.Popen(ps_args, text=True, stdout=subprocess.PIPE)
    df = pd.read_csv(
        p.stdout, 
        sep='\s+', 
        header=0,
        names=header,
        dtype=dtype,
        na_values='-',
        engine='python',
        on_bad_lines=bad_line_handler,
    )
    p.wait()

    return df


def run_ps_new(
    ps_format=[
        ('pid', None),
        ('tid', None),
        ('state', None),
        ('user', 50),
        ('pcpu', None),
        ('rss', None),
        ('cmd', None),
    ],
    int_keys=['pid', 'tid', 'rss'],
    float_keys=['pcpu'],
):
    df = run_ps_read_with_pandas(ps_format, int_keys, float_keys)
    tgroups, tmerge_df = postprocess_df(df)

    return tgroups, tmerge_df



# functions using psutil

def get_cpu_usages(interval=0.2):
    result = list()
    for idx, x in enumerate(psutil.cpu_times_percent(interval=interval, percpu=True)):
        pctsum = sum(x)
        x_asdict = x._asdict()
        new_data = dict((key, val / pctsum) for (key, val) in x_asdict.items())
        #max_key = max(x_asdict.items(), key=(lambda x: x[1]))[0]
        #string = '(' + ', '.join(f'{key}={val}' for (key, val) in new_data.items()) + ')'
        result.append(new_data)

    return result


def get_mem_usage(proc=None, pid=None):
    """Returns in kilobytes
    By using pss, shared memory sizes are not reduntantly counted. Useful for Rstudio or Jupyter spawning subprocesses.
    """
    if proc is None:
        proc = psutil.Process(pid)

    #rss = 0
    pss = 0
    for child in ([proc] + proc.children(recursive=True)):
        meminfo = child.memory_full_info()
        #rss += meminfo.rss
        pss += meminfo.pss
    
    #rss /= 1024**2
    pss /= 1024**2

    #return rss, pss
    return pss
        

def check_kernel_process(proc):
    return proc.ppid() == 2


def get_cputimes(proclist):
    return [x.cpu_times() for x in proclist]


def diff_cputimes_helper(key, cputimes_begin, cputimes_end, interval):
    arr = np.fromiter(
        ((getattr(x2, key) - getattr(x1, key)) for (x2, x1) in zip(cputimes_end, cputimes_begin)),
        dtype=float,
    )
    return 100 * (arr / interval)


def diff_cputimes(cputimes_begin, cputimes_end, interval):
    pcpus_user = diff_cputimes_helper('user', cputimes_begin, cputimes_end, interval)
    pcpus_sys = diff_cputimes_helper('system', cputimes_begin, cputimes_end, interval)
    pcpus_io = diff_cputimes_helper('iowait', cputimes_begin, cputimes_end, interval)
    return {
        'user': pcpus_user,
        'system': pcpus_sys,
        'iowait': pcpus_io,
    }


def get_pcpus(proclist, interval=0.2):
    cputimes_begin = get_cputimes(proclist)
    time.sleep(interval)
    cputimes_end = get_cputimes(proclist)
    result = diff_cputimes(cputimes_begin, cputimes_end, interval)
    return result


def get_iocounters(proclist):
    return [x.io_counters() for x in proclist]


def diff_iocounters_helper(key, iocounters_begin, iocounters_end, interval):
    iterator = (
        (getattr(x2, key) - getattr(x1, key))
        for (x2, x1) in zip(iocounters_end, iocounters_begin)
    )
    return np.fromiter(iterator, dtype=float) / interval


def diff_iocounters(iocounters_begin, iocounters_end, interval):
    read_rates = diff_iocounters_helper('read_bytes', iocounters_begin, iocounters_end, interval)
    write_rates = diff_iocounters_helper('write_bytes', iocounters_begin, iocounters_end, interval)
    return {
        'read': read_rates,
        'write': write_rates,
    }


def get_iorates(proclist, interval=0.2):
    """Args:
        proc: psutil.Process object
    Returns:
        bytes / sec
    """
    iocnts_begin = get_iocounters(proclist)
    time.sleep(interval)
    iocnts_end = get_iocounters(proclist)
    iorates = diff_iocounters(iocounters_begin, iocounters_end, interval)
    return iorates


def get_pcpus_iorates(proclist, interval=0.2):
    cputimes_begin = get_cputimes(proclist)
    iocnts_begin = get_iocounters(proclist)
    time.sleep(interval)
    cputimes_end = get_cputimes(proclist)
    iocnts_end = get_iocounters(proclist)

    pcpus = diff_cputimes(cputimes_begin, cputimes_end, interval)
    iorates = diff_iocounters(iocounters_begin, iocounters_end, interval)
    return pcpus, iorates


RD_STATUSES = [psutil.STATUS_RUNNING, psutil.STATUS_DISK_SLEEP]


def get_procinfo(
    interval=0.2,
):
    procinfo = list()
    for proc in psutil.process_iter(
        attrs=(
            'cmdline',
            'memory_full_info',
            #'name',
            'pid',
            #'status',
            'threads',
            'username',
        ),
    ):
        infoitem = dict()
        #infoitem['name'] = proc.info['name']
        infoitem['process'] = proc
        infoitem['pid'] = proc.info['pid']
        infoitem['user'] = proc.info['username']
        infoitem['cmd'] = proc.info['cmdline']
        infoitem['tids'] = [x.id for x in proc.info['threads']]
        infoitem['pss_bytes'] = proc.info['memory_full_info'].pss

        infoitem['thread_states'] = list()
        for x in infoitem['tids']:
            try:
                state = psutil.Process(x).status()
            except psutil.NoSuchProcess:
                pass
            else:
                infoitem['thread_states'].append(state)
        infoitem['num_RD'] = sum((x in RD_STATUSES) for x in infoitem['thread_states'])

        procinfo.append(infoitem)

    # get pcpus
    all_pcpus = get_pcpus([x['process'] for x in procinfo], interval=interval)
    for infoitem, pcpu_u, pcpu_s, pcpu_i in zip(
        procinfo,
        all_pcpus['user'],
        all_pcpus['system'],
        all_pcpus['iowait'],
    ):
        infoitem['pcpu_user'] = pcpu_u
        infoitem['pcpu_system'] = pcpu_s
        infoitem['pcpu_iowait'] = pcpu_i
        infoitem['pcpu_total'] = pcpu_u + pcpu_s + pcpu_i

    procinfo_dict = {x['pid']: x for x in procinfo}

    return procinfo, procinfo_dict

    
