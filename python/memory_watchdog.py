import os
import pprint
import time
import signal
import socket


NODELIST = [
    'bnode0',
    'bnode1',
    'bnode2',
    'bnode3',
    'bnode4',
    'bnode5',
    'bnode6',
    'bnode7',
    'bnode8',
    'bnode9',
    'bnode10',
    'bnode11',
    'bnode12',
    'bnode13',
    'bnode14',
    'bnode15',
    'bnode16',
]


def argument_parsing():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--logdir',
        help=f'Directory where log files are stored.',
        required=True,
        dest='logdir',
    )

    parser.add_argument(
        '--beginkill-threshold',
        help=f'Factor to be multiplied to the total memory, which will serve as a threshold above which killing will begin',
        default=0.9,
        type=float,
        dest='threshold_beginkill',
    )
    parser.add_argument(
        '--stopkill-threshold',
        help=f'Factor to be multiplied to the total memory, which will serve as a threshold below which killing will stop',
        default=0.8,
        type=float,
        dest='threshold_stopkill',
    )
    parser.add_argument(
        '--monitor-interval',
        help=f'Time interval (in seconds) between each CPU monitor cycle',
        default=60,
        type=float,
        dest='monitor_interval',
    )
    parser.add_argument(
        '--kill-timeout',
        help=f'Time (in seconds) to wait after sending SIGTERM to a process',
        default=5,
        type=float,
        dest='kill_timeout',
    )

    args = parser.parse_args()
    return args


def check_excessive_memuse(threshold_factor):
    memuse_fraction = psutil.virtual_memory().percent / 100
    return memuse_fraction >= threshold_factor


def kill_mem_overuser(timeout, threshold_factor, memtype):
    if memtype == 'pss':
        df_col = 'pss_bytes'
    elif memtype == 'rss':
        df_col = 'rss_bytes'
    else:
        raise Exception(f'"memtype" must be either pss or rss')

    all_df, byuser_dfs = utils.get_byuser_mem()
    if 'root' in byuser_dfs:
        del byuser_dfs['root']

    maxuser = max(
        byuser_dfs.items(),
        key=(lambda x: x[1][df_col].sum())
    )[0]
    maxuser_procs = byuser_dfs[maxuser].sort_values(df_col, axis=0, ascending=False)

    killed_procs = list()
    for idx, row in maxuser_procs.iterrows():
        killed_procs.append(row)
        rowdict = row.to_dict()
        utils.kill_proc([rowdict['pid']], timeout=timeout)
        print(f'Killed a process: {rowdict}')

        if not check_excessive_memuse(threshold_factor=threshold_factor):
            break

    killed_procs = pd.DataFrame.from_records(killed_procs)
    return killed_procs


def monitor(
    logdir,
    threshold_beginkill=0.9, 
    threshold_stopkill=0.8, 
    memtype='pss', 
    monitor_interval=60, 
    kill_timeout=5,
):
    hostname = socket.gethostname()
    while True:
        utils.print_timestamp(f'{hostname}: Checking memory usage')
        if check_excessive_memuse(threshold_factor=threshold_beginkill):
            killed_procs = kill_mem_overuser(
                timeout=kill_timeout, 
                threshold_factor=threshold_stopkill, 
                memtype=memtype,
            )
            utils.write_log(killed_procs, logdir)
            utils.print_timestamp(f'{hostname}: Finished killing. Sleeping for {monitor_interval} seconds')
        else:
            utils.print_timestamp(f'{hostname}: Memory usage below threshold. Sleeping for {monitor_interval} seconds')
        time.sleep(monitor_interval)


def main():
    args = argument_parsing()
    os.makedirs(args.logdir, exist_ok=True)

    plist = [
        utils.run_over_ssh(x, monitor, kwargs=vars(args))
        for x in NODELIST
    ]
    for p in plist:
        p.wait()


if __name__ == '__main__':
    main()


