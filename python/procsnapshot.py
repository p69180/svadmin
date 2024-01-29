import collections
import multiprocessing
import socket

import numpy as np
import pandas as pd

import utils
import psutil_funcs


SNAPSHOT_DF_KEYS = [
    'hostname',
    'user',

    'pcpu_user',
    'pcpu_system',
    'pcpu_iowait',
    'pcpu_total',
    'num_RD',

    'pss_bytes',
    'rss_bytes',

    'read B/s',
    'write B/s',
    'read B',
    'write B',

    'cmd',
]


class ProcessInfo(collections.UserDict):
    allowed_keys = (
        'hostname',
        'pid',
        'user',
        'cmd',

        'pss_bytes',
        'rss_bytes',

        'tids',
        'thread_states',
        'num_RD',

        'pcpu_user',
        'pcpu_system',
        'pcpu_iowait',
        'pcpu_total',
        
        'read B/s',
        'write B/s',
        'read B',
        'write B',
    )

    def check_key(self, key):
        if key not in self.__class__.allowed_keys: 
            raise Exception(f'Allowed keys: {self.__class__.allowed_keys}')

    def __getitem__(self, key):
        self.check_key(key)
        try:
            return super().__getitem__(key)
        except KeyError:
            return pd.NA

    def __setitem__(self, key, val):
        self.check_key(key)
        super().__setitem__(key, val)


class ProcessSnapshot:
    @classmethod
    def from_psutil(cls, pcpu=True, IO=True, interval=0.2):
        result = cls()
        result.psutil_procs = list(
            psutil_funcs.run_process_iter(root=utils.check_root())
        )
        result.set_procinfos_psutil(
            pcpu=pcpu, IO=IO, interval=interval,
        )
        result.set_df()
        return result

    def set_procinfos_psutil(self, pcpu=True, IO=True, interval=0.2):
        self.procinfos = list()
        self.procinfos.extend(
            (
                ProcessInfo(psutil_funcs.into_procinfo_noncpu(proc)) 
                for proc in self.psutil_procs
            )
        )
        self.add_pcpu_io_with_psutil(
            pcpu=pcpu, IO=IO, interval=interval,
        )

    def set_df(self):
        df = pd.DataFrame.from_records(
            [
                {key: dic[key] for key in SNAPSHOT_DF_KEYS}
                for dic in self.procinfos
            ]
        )

        df['pss_GB'] = df['pss_bytes'] / 1024**3
        df['rss_GB'] = df['rss_bytes'] / 1024**3
        df['read MB/s'] = df['read B/s'] / 1024**2
        df['write MB/s'] = df['write B/s'] / 1024**2
        df['read MB'] = df['read B'] / 1024**2
        df['write MB'] = df['write B'] / 1024**2
        
        df.set_index(['hostname', 'user'], inplace=True)

        self.df = df

    def add_pcpu_io_with_psutil(self, pcpu=True, IO=True, interval=0.2):
        if pcpu and IO:
            pcpus, iorates = psutil_funcs.get_pcpus_iorates(self.psutil_procs, interval=interval)
        elif pcpu and (not IO):
            pcpus = psutil_funcs.get_pcpus(self.psutil_procs, interval=interval)
            iorates = None
        elif (not pcpu) and IO:
            pcpus = None
            iorates = psutil_funcs.get_iorates(self.psutil_procs, interval=interval)
        else:
            pcpus = None
            iorates = None

        # assign pcpus
        if pcpus is None:
            for x in self.procinfos:
                x['pcpu_user'] = pd.NA
                x['pcpu_system'] = pd.NA
                x['pcpu_iowait'] = pd.NA
                x['pcpu_total'] = pd.NA
        else:
            for idx, x in enumerate(self.procinfos):
                x['pcpu_user'] = pcpus['user'][idx]
                x['pcpu_system'] = pcpus['system'][idx]
                x['pcpu_iowait'] = pcpus['iowait'][idx]
                try:
                    x['pcpu_total'] = (
                        x['pcpu_user']
                        + x['pcpu_system']
                        + x['pcpu_iowait']
                    )
                except:
                    print(idx)
                    print(pcpus['user'][idx])
                    print(pcpus['system'][idx])
                    print(pcpus['iowait'][idx])
                    print(x['pcpu_user'])
                    print(x['pcpu_system'])
                    print(x['pcpu_iowait'])
                    raise
                        

        # assign iorates
        if iorates is None:
            for x in self.procinfos:
                x['read B/s'] = pd.NA
                x['write B/s'] = pd.NA
                x['read B'] = pd.NA
                x['write B'] = pd.NA
        else:
            for idx, x in enumerate(self.procinfos):
                x['read B/s'] = iorates['read B/s'][idx]
                x['write B/s'] = iorates['write B/s'][idx]
                x['read B'] = iorates['read B'][idx]
                x['write B'] = iorates['write B'][idx]


def get_snapshot_df_onenode_psutil(pcpu=True, IO=True, interval=0.2):
    snapshot = ProcessSnapshot.from_psutil(pcpu=pcpu, IO=IO, interval=interval)
    return snapshot.df


def get_snapshot_df_psutil(nodelist='all', pcpu=True, IO=True, interval=0.2):
    if nodelist == 'current':
        nodelist = [socket.gethostname()]
    elif nodelist == 'all':
        nodelist = utils.ALL_NODES
    else:
        nodelist = list(nodelist)

    with multiprocessing.Pool() as pool:
        argsiter = (
            (
                host, 
                get_snapshot_df_onenode_psutil, 
                [pcpu, IO, interval]
            )
            for host in nodelist
        )
        node_results = pool.starmap(utils.run_over_ssh, argsiter)

    return pd.concat(
        [x for x in node_results if x is not None], 
        axis=0,
    )


get_snapshot_df = get_snapshot_df_psutil
