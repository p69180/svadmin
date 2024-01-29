import os
import pprint
import argparse
import socket

import utils


MAX_PROCS_DF_KEYS = [
    'pid',
    'user',
    'pcpu_total',
    'num_RD',

    'rss_GB',
    'pss_GB',

    'read_MB_per_sec',
    'written_MB_per_sec',

    'cmd',
]


GROUPED_DF_KEYS = [
    'pcpu_user',
    'pcpu_system',
    'pcpu_iowait',
    'pcpu_total',
    'num_RD',

    'rss_GB',
    'pss_GB',

    'read_MB_per_sec',
    'written_MB_per_sec',
]


def argument_parsing():
    def nodes_postprocess(x):
        if x is None:
            return [socket.gethostname()]
        else:
            return x.split(',')

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--nodes',
        help=f'Comma-separated string indicating target nodes. (e.g. bnode2,bnode4) Default: current node.',
        default=None,
        type=nodes_postprocess,
        dest='nodes',
    )
    parser.add_argument(
        '--group',
        help=f'If set, printed dataframe is grouped by user and hostname.',
        action='store_true',
        dest='group',
    )
    parser.add_argument(
        '-i', '--interval', 
        help=f'Time interval (in seconds) to collect cpu usage data.',
        default=1,
        type=float,
        dest='interval',
    )
    parser.add_argument(
        '-n', '--num-maxproc', 
        help=f'The number of processes with maximal cpu/memory usage to print',
        default=5,
        type=int,
        dest='num_maxproc',
    )
    parser.add_argument(
        '--save',
        help=f'If set, snapshot result is saved as a tsv file to "./snapshot.tsv.gz"',
        action='store_true',
        dest='save',
    )

    args = parser.parse_args()
    return args


def main():
    args = argument_parsing()

    snapshot_df = utils.get_snapshot_df(nodes=args.nodes)
    grouped_snapshot_df = snapshot_df.groupby(['hostname', 'user'])[GROUPED_DF_KEYS].sum()

    snapshot = utils.get_byuser_snapshot(interval=args.interval, gross_cpu_percents=False)
    maxcpu_df = utils.pick_maxcpu_procs(snapshot['proc_snapshot_df'], n=args.num_maxproc)
    maxmem_df = utils.pick_maxmem_procs(snapshot['proc_snapshot_df'], n=args.num_maxproc)

    print(snapshot['grouped_df'])
    print()
    print(f'{args.num_maxproc} processes with largest pcpu:') 
    print(maxcpu_df.loc[:, MAX_PROCS_DF_KEYS])
    print()
    print(f'{args.num_maxproc} processes with largest memory:') 
    print(maxmem_df.loc[:, MAX_PROCS_DF_KEYS])

    if args.save:
        savepath = './snapshot.tsv.gz'
        if not os.path.exists(savepath):
            snapshot['proc_snapshot_df'].to_csv(savepath, sep='\t', header=True, index=False)
        else:
            print(f'File {repr(savepath)} already exists. Snapshot dataframe is not written.')
    

if __name__ == '__main__':
    main()

