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


ALL_NODES = [f'bnode{x}' for x in range(17)]

SNAPSHOT_DF_KEYS = [
    'hostname',
    'user',

    'pcpu_user',
    'pcpu_system',
    'pcpu_iowait',
    'pcpu_total',
    'num_RD',

    'rss_GB',
    'pss_GB',

    'read_MB_per_sec',
    'written_MB_per_sec',

    'cmd',
]


SNAPSHOT_DF_GROUPBY_KEYS = [
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


##########
# basics #
##########

def make_tmpfile_path(where=os.getcwd(), prefix=None, suffix=None):
    fd, path = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=where)
    os.close(fd)
    return path


def get_username():
    return pwd.getpwuid(os.getuid()).pw_name


def check_root():
    return os.geteuid() == 0


def rootcheck():
    if not check_root():
        print(f'User must be root.')
        sys.exit(1)


def rootcheck_ask_force(prompt=None):
    if prompt is None:
        prompt = f'User is not root. If you want to proceed anyway, press "y". '

    if not check_root():
        answer = input(prompt)
        if answer == 'y':
            pass
        else:
            sys.exit(1)


################
# run over ssh #
################

def run_over_ssh(hostname, func, args=tuple(), kwargs=dict()):
    # pickle args and kwargs
    uniqid = uuid.uuid4()
    args_pklpath = make_tmpfile_path(
        prefix=f'runoverssh_args_{uniqid}_', suffix='.pickle',
    )
    kwargs_pklpath = make_tmpfile_path(
        prefix=f'runoverssh_kwargs_{uniqid}_', suffix='.pickle',
    )
    result_pklpath = make_tmpfile_path(
        prefix=f'runoverssh_result_{uniqid}_', suffix='.pickle',
    )
    with open(args_pklpath, 'wb') as f:
        pickle.dump(args, f)
    with open(kwargs_pklpath, 'wb') as f:
        pickle.dump(kwargs, f)

    # make parameters
    module = inspect.getmodule(func)
    assert hasattr(module, '__file__')
    module_path = os.path.abspath(module.__file__)
    module_dir = os.path.dirname(module_path)
    module_name = re.sub(r'\.py$', '', os.path.basename(module_path))

    funcname = func.__name__

    python = sys.executable
    remotearg_pycmd = textwrap.dedent(
        f'''\
        import sys
        import os
        import importlib
        import pickle
        import json
        sys.path.append({repr(module_dir)})
        module = importlib.import_module({repr(module_name)})
        func = getattr(module, {repr(funcname)})
        with open({repr(args_pklpath)}, 'rb') as f:
            args = pickle.load(f)
        with open({repr(kwargs_pklpath)}, 'rb') as f:
            kwargs = pickle.load(f)
        result = func.__call__(*args, **kwargs)
        with open({repr(result_pklpath)}, 'wb') as f:
            pickle.dump(result, f)
        '''
    )
        # tmpfile paths for pickle are removed here
    remoteargs = shlex.join([python, '-c', remotearg_pycmd])
    
    p = subprocess.run(
        ['ssh', hostname, remoteargs],
        capture_output=True,
        text=True,
        check=False,
    )

    if p.returncode == 0:
        with open(result_pklpath, 'rb') as f:
            result = pickle.load(f)
    else:
        this_func_name = inspect.stack()[0].function
        msg = f'"{this_func_name}" failed; hostname={hostname}, function={func}, stderr={p.stderr}'
        print(msg)
        #warnings.warn(msg)
        result = None

    os.remove(args_pklpath)
    os.remove(kwargs_pklpath)
    os.remove(result_pklpath)

    return result


############
# logutils #
############

def get_timestamp():
    """Returns a string like 'KST 2021-12-06 11:51:36'"""
    dt = datetime.datetime.now().astimezone()
    return f'{str(dt.tzinfo)} {str(dt).split(".")[0]}'


def print_err(*args, stderr=True, files=None, **kwargs):
    """Args:
        stderr: (Bool) Whether to write to stderr
        files: A list of file paths to which message is written
    """
    if stderr:
        print(*args, file=sys.stderr, flush=True, **kwargs)

    if files is not None:
        for fname in files:
            with open(fname, 'a') as f:
                print(*args, file=f, flush=True, **kwargs)


def print_timestamp(*args, **kwargs):
    print_err(f'[{get_timestamp()}]', *args, **kwargs)


########
# kill #
########

def kill_proc(pidlist, timeout=3, raise_on_absent=False):
    proclist = [psutil.Process(x) for x in pidlist]
    for proc in proclist:
        try:
            proc.terminate()
        except psutil.NoSuchProcess:
            if raise_on_absent:
                raise
            else:
                pass

    gone, alive = psutil.wait_procs(proclist, timeout=timeout, callback=None)
    if len(alive) > 0:
        for proc in alive:
            try:
                proc.kill()
            except psutil.NoSuchProcess:
                if raise_on_absent:
                    raise
                else:
                    pass
        time.sleep(1)


######


def init_tgroup(linedict):
    tgroup = dict()
    tgroup['all_items'] = list()
    tgroup['threads'] = list()
    tgroup['leader'] = linedict
    return tgroup


def finalize_tgroup(tgroup):
    assert len(set(x['user'] for x in tgroup)) == 1
    #assert len(set(x['user'] for x in tgroup['all_items'])) == 1
    #tgroup['user'] = tgroup['leader']['USER']
    #tgroup['pid'] = tgroup['leader']['PID']
    #tgroup['cmd'] = tgroup['leader']['CMD']
    #tgroup['pmem'] = tgroup['leader']['%MEM']
    #tgroup['rss_kb'] = tgroup['leader']['RSS']


def ps_linedict_sanitycheck(linedict):
    if linedict['pid'] is None:  # threads
        assert all(
            (linedict[key] is None)
            for key in ('cmd', 'rss')
        ), f'{linedict}'
    else:  # tgroup leader process
        assert all(
            (linedict[key] is None)
            for key in ('state', 'tid')
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
    # cmd must come at the end
    if 'cmd' in columns:
        assert columns.index('cmd') == len(columns) - 1

    init = True
    for line in stdout_split[1:]:
        linedict = make_ps_linedict(line, columns)

        if linedict['pid'] is not None:
            if init:
                init = False
            else:
                finalize_tgroup(tgroup)
                yield tgroup
            #tgroup = init_tgroup(linedict)
            tgroup = list()

        ps_linedict_sanitycheck(linedict)
        tgroup.append(linedict)
        #tgroup['all_items'].append(linedict)
        #if linedict['PID'] is None:
        #    tgroup['threads'].append(linedict)


def get_proc_snapshot_ps():
    p = subprocess.run(
        [
            'ps', '-mA', '-o', 
            'pid=pid,tid=tid,state=state,user:50=user,pcpu=pcpu,rss=rss,cmd=cmd',
        ],
        text=True,
        capture_output=True,
    )
    stdout_split = p.stdout.strip().split('\n')
    columns = stdout_split[0].split()

    proc_snapshot = list()
    for tgroup in ps_output_iterator(stdout_split, columns):
        procinfo = dict()
        procinfo['pid'] = tgroup[0]['pid']

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


DEFAULT_PS_FORMAT = [
    ('pid', None, 'pid'),
    ('tid', None, 'tid'),
    ('state', None, 'state'),
    ('user', 50, 'user'),
    ('pcpu', None, 'pcpu'),
    ('rss', None, 'rss'),
    ('cmd', None, 'cmd'),
]


def make_ps_args(
    format_names=None,
    include_threads=False,
):
    assert len(format_names) > 0

    if format_names is None:
        format_names = ('pid', 'tid', 'state', 'user', 'pcpu', 'rss', 'cmd')

    ps_format = list()
    for x in format_names:
        if x == 'user':
            ps_format.append((x, 50, x))
        else:
            ps_format.append((x, None, x))

    result = [
        'ps', 
        ('-mA' if include_threads else '-A'),
    ]
    for x in ps_format:
        arg = x[0]
        if x[1] is not None:
            arg += f':{x[1]}'
        if x[2] is not None:
            arg += f'={x[2]}'
        result.extend(('-o', arg))

    return result


def run_ps_read_with_pandas(
    format_names=None,
    include_threads=False,
    int_keys=['pid', 'tid', 'rss'],
    float_keys=['pcpu'],
):
    ps_args = make_ps_args(
        format_names=format_names,
        include_threads=include_threads,
    )
    p = subprocess.run(ps_args, text=True, capture_output=True)
    header = p.stdout.split('\n')[0].split()

    # prepare pandas parameters
    if 'cmd' in header:
        assert header.index('cmd') == len(header) - 1
    dtype = collections.defaultdict(pd.StringDtype)
    for key in int_keys:
        if key in header:
            dtype[key] = pd.Int64Dtype()
    for key in float_keys:
        if key in header:
            dtype[key] = pd.Float64Dtype()

    def bad_line_handler(bad_line):
        """For handling cmd strings"""
        assert len(bad_line) > len(header)
        new_line = list()
        new_line.extend(bad_line[:(len(header) - 1)])
        new_line.append(' '.join(bad_line[(len(header) - 1):]))
        return new_line
            
    # run ps and read with pandas
    #p = subprocess.Popen(ps_args, text=True, stdout=subprocess.PIPE)
    df = pd.read_csv(
        #p.stdout, 
        io.StringIO(p.stdout),
        sep=r'\s+', 
        header=0,
        names=header,
        dtype=dtype,
        na_values='-',
        engine='python',
        on_bad_lines=bad_line_handler,
    )
    #p.wait()

    return df


def get_byuser_pcpu():
    all_df = run_ps_read_with_pandas(
        format_names=('pid', 'pcpu', 'user', 'cmd'),
        include_threads=False,
    )
    byuser_dfs = dict(
        (key, subdf) for key, subdf in all_df.groupby('user')
    )
    return all_df, byuser_dfs


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




#################################
# functions using ps and pandas #
#################################

DEFAULT_PS_FORMAT = [
    ('pid', None, 'pid'),
    ('tid', None, 'tid'),
    ('state', None, 'state'),
    ('user', 50, 'user'),
    ('pcpu', None, 'pcpu'),
    ('rss', None, 'rss'),
    ('cmd', None, 'cmd'),
]


def make_ps_args(
    format_names=None,
    include_threads=False,
):
    assert len(format_names) > 0

    if format_names is None:
        format_names = ('pid', 'tid', 'state', 'user', 'pcpu', 'rss', 'cmd')

    ps_format = list()
    for x in format_names:
        if x == 'user':
            ps_format.append((x, 50, x))
        else:
            ps_format.append((x, None, x))

    result = [
        'ps', 
        ('-mA' if include_threads else '-A'),
    ]
    for x in ps_format:
        arg = x[0]
        if x[1] is not None:
            arg += f':{x[1]}'
        if x[2] is not None:
            arg += f'={x[2]}'
        result.extend(('-o', arg))

    return result


def run_ps_read_with_pandas(
    format_names=None,
    include_threads=False,
    int_keys=['pid', 'tid', 'rss'],
    float_keys=['pcpu'],
):
    ps_args = make_ps_args(
        format_names=format_names,
        include_threads=include_threads,
    )
    p = subprocess.run(ps_args, text=True, capture_output=True)
    header = p.stdout.split('\n')[0].split()

    # prepare pandas parameters
    if 'cmd' in header:
        assert header.index('cmd') == len(header) - 1
    dtype = collections.defaultdict(pd.StringDtype)
    for key in int_keys:
        if key in header:
            dtype[key] = pd.Int64Dtype()
    for key in float_keys:
        if key in header:
            dtype[key] = pd.Float64Dtype()

    def bad_line_handler(bad_line):
        assert len(bad_line) > len(header)
        new_line = list()
        new_line.extend(bad_line[:(len(header) - 1)])
        new_line.append(' '.join(bad_line[(len(header) - 1):]))
        return new_line
            
    # run ps and read with pandas
    #p = subprocess.Popen(ps_args, text=True, stdout=subprocess.PIPE)
    df = pd.read_csv(
        #p.stdout, 
        io.StringIO(p.stdout),
        sep=r'\s+', 
        header=0,
        names=header,
        dtype=dtype,
        na_values='-',
        engine='python',
        on_bad_lines=bad_line_handler,
    )
    #p.wait()

    return df


def get_byuser_pcpu():
    all_df = run_ps_read_with_pandas(
        format_names=('pid', 'pcpu', 'user', 'cmd'),
        include_threads=False,
    )
    byuser_dfs = dict(
        (key, subdf) for key, subdf in all_df.groupby('user')
    )
    return all_df, byuser_dfs


def get_load_snapshot():
    all_df = run_ps_read_with_pandas(
        format_names=('state',),
        include_threads=True,
    )
    return all_df['state'].isin(['R', 'D']).sum()


        
# pick max mem/cpu processes

def pick_maxcpu_procs(proc_snapshot_df, n=1):
    sorted_df = proc_snapshot_df.sort_values(by='pcpu_total', axis=0, ascending=False)
    return sorted_df.head(n)


def pick_maxmem_procs(proc_snapshot_df, n=1):
    sorted_df = proc_snapshot_df.sort_values(by='pss_GB', axis=0, ascending=False)
    return sorted_df.head(n)




########################
# logging kill results #
########################

def write_log(killed_procs, logdir):
    host_logdir = os.path.join(logdir, socket.gethostname())
    os.makedirs(host_logdir, exist_ok=True)
    logpath = os.path.join(host_logdir, datetime.datetime.now().isoformat())
    killed_procs.to_csv(logpath, sep='\t', header=True, index=False)


