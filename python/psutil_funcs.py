import sys
import os
import subprocess
import pprint
import collections
import itertools
import time
import operator
import io
import inspect
import socket
import re
import datetime
import tempfile
import pickle
import uuid
import shlex
import textwrap
import json
import warnings
import pwd
import multiprocessing

import psutil
import numpy as np
import pandas as pd


RD_STATUSES = [psutil.STATUS_RUNNING, psutil.STATUS_DISK_SLEEP]


def get_byuser_mem():
    df_data = list()
    for proc in psutil.process_iter(
        attrs=(
            'memory_full_info',
            'cmdline',
            'username',
        ),
    ):
        rowdict = dict()
        rowdict['pid'] = proc.pid
        rowdict['pss_bytes'] = proc.info['memory_full_info'].pss
        rowdict['pss_GB'] = round(rowdict['pss_bytes'] / 1024**3, 3)
        rowdict['rss_bytes'] = proc.info['memory_full_info'].rss
        rowdict['rss_GB'] = round(rowdict['rss_bytes'] / 1024**3, 3)
        rowdict['user'] = proc.info['username']
        rowdict['cmd'] = ' '.join(proc.info['cmdline'])

        df_data.append(rowdict)

    all_df = pd.DataFrame.from_records(df_data)
    byuser_dfs = dict(
        (key, subdf) for key, subdf in all_df.groupby('user')
    )

    return all_df, byuser_dfs


def get_cpu_usages(interval=0.2):
    result = list()
    for idx, x in enumerate(psutil.cpu_times_percent(interval=interval, percpu=True)):
        pctsum = sum(x)
        x_asdict = x._asdict()
        new_data = dict((key, val / pctsum) for (key, val) in x_asdict.items())
        # max_key = max(x_asdict.items(), key=(lambda x: x[1]))[0]
        # string = '(' + ', '.join(f'{key}={val}' for (key, val) in new_data.items()) + ')'
        result.append(new_data)

    return result


def get_mem_usage(proc=None, pid=None):
    """Returns in kilobytes
    By using pss, shared memory sizes are not reduntantly counted. Useful for Rstudio or Jupyter spawning subprocesses.
    """
    if proc is None:
        proc = psutil.Process(pid)

    rss = 0
    pss = 0
    for child in ([proc] + proc.children(recursive=True)):
        meminfo = child.memory_full_info()
        rss += meminfo.rss
        pss += meminfo.pss

    rss /= 1024**2
    pss /= 1024**2

    return rss, pss
    # return pss


def check_kernel_process(proc):
    return proc.ppid() == 2


###

# helpers

def get_process_data(proclist, methodname):
    result = list()
    for x in proclist:
        try:
            item = getattr(x, methodname)()
        except psutil.NoSuchProcess:
            item = None
        result.append(item)
    return result


def get_cputimes(proclist):
    return get_process_data(proclist, 'cpu_times')


def get_iocounters(proclist):
    return get_process_data(proclist, 'io_counters')


def diff_helper(attrname, data_begin, data_end, interval):
    diffresult = list()
    for x1, x2 in zip(data_begin, data_end):
        if (x2 is None) or (x1 is None):
            diffitem = np.nan
        else:
            diffitem = getattr(x2, attrname) - getattr(x1, attrname)
        diffresult.append(diffitem)

    amount = np.asarray(diffresult, dtype=float)
    rate = amount / interval
    return {'amount': amount, 'rate': rate}


def diff_cputimes(cputimes_begin, cputimes_end, interval):
    #print(diff_helper('user', cputimes_begin, cputimes_end, interval)['rate'])
    return {
        'user': (100 * diff_helper('user', cputimes_begin, cputimes_end, interval)['rate']),
        'system': (100 * diff_helper('system', cputimes_begin, cputimes_end, interval)['rate']),
        'iowait': (100 * diff_helper('iowait', cputimes_begin, cputimes_end, interval)['rate']),
    }


def diff_iocounters(iocounters_begin, iocounters_end, interval):
    read_diffresult = diff_helper('read_bytes', iocounters_begin, iocounters_end, interval)
    write_diffresult = diff_helper('write_bytes', iocounters_begin, iocounters_end, interval)

    return {
        'read B/s': read_diffresult['rate'],
        'write B/s': write_diffresult['rate'],
        'read B': read_diffresult['amount'],
        'write B': write_diffresult['amount'],
    }


# main ones

def get_pcpus(proclist, interval=0.2):
    cputimes_begin = get_cputimes(proclist)
    time.sleep(interval)
    cputimes_end = get_cputimes(proclist)
    result = diff_cputimes(cputimes_begin, cputimes_end, interval)
    return result


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
    iorates = diff_iocounters(iocnts_begin, iocnts_end, interval)
    return pcpus, iorates


def get_thread_states(thread_ids):
    result = list()
    for x in thread_ids:
        try:
            state = psutil.Process(x).status()
        except psutil.NoSuchProcess:
            pass
        else:
            result.append(state)
    return result


def into_procinfo_helper(proc, valgetter):
    try:
        val = valgetter(proc)
    except:
        val = None
    return val


def into_procinfo_noncpu(proc):
    """Makes a source dictionary for ProcessInfo instance"""

    assert isinstance(proc, psutil.Process)
    assert hasattr(proc, 'info')

    procinfo_dict = dict()
    procinfo_dict['hostname'] = socket.gethostname()
    procinfo_dict['pid'] = proc.pid
    procinfo_dict['user'] = proc.info['username']
    procinfo_dict['cmd'] = ' '.join(proc.info['cmdline'])

    if 'memory_full_info' in proc.info:  # when run with root
        procinfo_dict['pss_bytes'] = into_procinfo_helper(
            proc,
            (lambda x: x.info['memory_full_info'].pss),
        )
        procinfo_dict['rss_bytes'] = into_procinfo_helper(
            proc,
            (lambda x: x.info['memory_full_info'].rss),
        )
    else:
        procinfo_dict['rss_bytes'] = into_procinfo_helper(
            proc,
            (lambda x: x.info['memory_info'].rss),
        )

    procinfo_dict['tids'] = into_procinfo_helper(
        proc,
        (lambda x: [x.id for x in proc.info['threads']]),
    )
    procinfo_dict['thread_states'] = get_thread_states(procinfo_dict['tids'])
    procinfo_dict['num_RD'] = sum(
        (x in RD_STATUSES)
        for x in procinfo_dict['thread_states']
    )

    return procinfo_dict


#def get_proc_snapshot_noncpu():
#    hostname = socket.gethostname()
#
#    proc_snapshot = list()
#    for proc in psutil.process_iter(
#        attrs=(
#            'cmdline',
#            'memory_full_info',
#            # 'name',
#            'pid',
#            # 'status',
#            'threads',
#            'username',
#        ),
#    ):
#        procinfo = dict()
#        # procinfo['name'] = proc.info['name']
#        procinfo['hostname'] = hostname
#        procinfo['pid'] = proc.info['pid']
#        procinfo['process'] = proc
#        procinfo['user'] = proc.info['username']
#        procinfo['cmd'] = proc.info['cmdline']
#        procinfo['tids'] = [x.id for x in proc.info['threads']]
#
#        if proc.info['memory_full_info'] is not None:
#            procinfo['rss_bytes'] = proc.info['memory_full_info'].rss
#            procinfo['rss_GB'] = round(procinfo['rss_bytes'] / 1024**3, 3)
#            procinfo['pss_bytes'] = proc.info['memory_full_info'].pss
#            procinfo['pss_GB'] = round(procinfo['pss_bytes'] / 1024**3, 3)
#
#        procinfo['thread_states'] = list()
#        for x in procinfo['tids']:
#            try:
#                state = psutil.Process(x).status()
#            except psutil.NoSuchProcess:
#                pass
#            else:
#                procinfo['thread_states'].append(state)
#        procinfo['num_RD'] = sum((x in RD_STATUSES)
#                                 for x in procinfo['thread_states'])
#
#        proc_snapshot.append(procinfo)
#
#    return proc_snapshot


def run_process_iter(root=True):
    attrs = [
        'cmdline',
        'pid',
        'threads',
        'username',
        ('memory_full_info' if root else 'memory_info'),
    ]
    return psutil.process_iter(attrs=attrs)


def get_proc_snapshot(interval=0.2, gross_cpu_percents=False):
    proc_snapshot = get_proc_snapshot_noncpu()

    proclist = [x['process'] for x in proc_snapshot]
    if gross_cpu_percents:
        _ = psutil.cpu_percent(percpu=True)
    all_pcpus, all_iorates = get_pcpus_iorates(proclist, interval=interval)
    if gross_cpu_percents:
        gross_pcpus = psutil.cpu_percent(percpu=True)
    else:
        gross_pcpus = None

    for (
        procinfo,
        pcpu_u,
        pcpu_s,
        pcpu_i,
        r_rate,
        w_rate,
    ) in zip(
        proc_snapshot,
        all_pcpus['user'],
        all_pcpus['system'],
        all_pcpus['iowait'],
        all_iorates['read'],
        all_iorates['write'],
    ):
        procinfo['pcpu_user'] = pcpu_u
        procinfo['pcpu_system'] = pcpu_s
        procinfo['pcpu_iowait'] = pcpu_i
        procinfo['pcpu_total'] = pcpu_u + pcpu_s + pcpu_i

        procinfo['read_bytes_per_sec'] = r_rate
        procinfo['written_bytes_per_sec'] = w_rate
        procinfo['read_MB_per_sec'] = round(r_rate / 1024**2, 3)
        procinfo['written_MB_per_sec'] = round(w_rate / 1024**2, 3)

    # make df
    proc_snapshot_df = pd.DataFrame.from_records(
        proc_snapshot).loc[:, SNAPSHOT_DF_KEYS]
    proc_snapshot_df.set_index(['hostname', 'user'], inplace=True)

    return proc_snapshot, proc_snapshot_df, gross_pcpus


def get_proc_snapshot_df_onenode(interval=0.2, gross_cpu_percents=False):
    proc_snapshot, proc_snapshot_df, gross_pcpus = get_proc_snapshot(
        interval=interval, gross_cpu_percents=gross_cpu_percents)
    return proc_snapshot_df


def group_procinfo(proc_snapshot):
    procinfo_bypid = dict((x['pid'], x) for x in proc_snapshot)
    procinfo_byuser = dict()
    for user, subiter in itertools.groupby(
        sorted(proc_snapshot, key=operator.itemgetter('user')),
        key=operator.itemgetter('user'),
    ):
        procinfo_byuser[user] = list(subiter)

    return procinfo_bypid, procinfo_byuser


def get_byuser_snapshot(interval=0.2, gross_cpu_percents=False):
    proc_snapshot, proc_snapshot_df, gross_pcpus = get_proc_snapshot(
        interval=interval,
        gross_cpu_percents=gross_cpu_percents,
    )
    grouped_df = proc_snapshot_df.groupby(['hostname', 'user'])[
        SNAPSHOT_DF_GROUPBY_KEYS].sum()
    sum_row = grouped_df.sum(axis=0).to_frame(name='SUM').T
    grouped_df = pd.concat(
        [grouped_df, sum_row],
        axis=0,
        ignore_index=False,
    )

    result = {
        'proc_snapshot': proc_snapshot,
        'proc_snapshot_df': proc_snapshot_df,
        'grouped_df': grouped_df,
        'gross_pcpus': gross_pcpus,  # a list with an element for each cpu
    }
    return result


def get_byuser_snapshot_wopandas(interval=0.2):
    proc_snapshot, proc_snapshot_df, gross_pcpus = get_proc_snapshot(
        interval=interval)
    procinfo_bypid, procinfo_byuser = group_procinfo(proc_snapshot)
    result = dict()
    for user, procinfo_list in procinfo_byuser.items():
        userdata = dict()
        for key in SNAPSHOT_DF_KEYS:
            userdata[key] = sum(x[key] for x in procinfo_list)
            result[user] = userdata

    return result


def get_snapshot_df(nodes='current'):
    if nodes == 'current':
        nodes = [socket.gethostname()]
    elif nodes == 'all':
        nodes = ALL_NODES
    else:
        nodes = list(nodes)

    # run main function over nodes
    with multiprocessing.Pool() as pool:
        argsiter = ((host, get_proc_snapshot_df_onenode) for host in nodes)
        # node_results = dict(zip(ALL_NODES, pool.starmap(run_over_ssh, argsiter)))
        node_results = pool.starmap(run_over_ssh, argsiter)

    # make result df
    df = pd.concat(
        [x for x in node_results if x is not None],
        axis=0,
    )

    return df
