import os
import subprocess
import pwd
import datetime
import shutil
import gzip

import storage_monitor_common
import utils


HEADER = [
    'filename',
    #'stat_result',
    'size_gb',
    'size_bytes',
    'username',
    #'lastmodify_dtobj',
    #'lastaccess_dtobj',
    'lastmodify',
    'lastaccess',
    'is_text',
]



def populate_parser(parser):
    def make_default_fdpath():
        fdpath = shutil.which('fd')
        if fdpath is None:
            raise Exception(f'Cannot find an executable of "fd".')
        return fdpath

    newerolder_help = (
        'Files whose modification time point is {} than this are included. '
        'If a duration is given (e.g. 10h, 1d, 35min), the time point compared is '
        'earlier than now by this value. Directly used as "{}" option of "fd" command.'
    )

    parser.add_argument(
        '--outdir', 
        default=None,
        help=f'Directory where results are saved. It must not exist in advance. If not given, default value is like: ./{{timestamp}}_newer_{{newer}}_older_{{older}}_size_{{size}}',
    )
    parser.add_argument(
        '--topdir', 
        default='/home/users',
        type=os.path.abspath,
        help=f'Top directory from where to begin search.',
    )
    parser.add_argument(
        '--threads',
        default=None,
        type=int,
        help=f'Number of jobs simultaneously run at a time. Default is the number of all available cpus.',
    )
    parser.add_argument(
        '--newer', 
        required=False,
        default=None,
        help=(newerolder_help.format('LATER', '--newer') + ' A recommended choice is "10d".'),
    )
    parser.add_argument(
        '--older', 
        required=False,
        default=None,
        help=newerolder_help.format('EARLIER', '--older'),
    )
    parser.add_argument(
        '--size', 
        default=None,
        help=(
            f'Size limit of included files. "+/-" means greater/less than or equal to. '
            f'Allowed units are "b, k, m, g, t, ki, mi, gi, ti". '
            f'Directly used as "--size" option of "fd" command. '
            f'A recommended choice is "+1g".'
        ),
    )
#    parser.add_argument(
#        '--depth', 
#        default=2,
#        type=int,
#        help=f'Result files are created for each N-depth files relative to <topdir>. Must be a non-negative integer. When depth is 0, only one result file is created. When depth is 1, result files will be created for each output of "fd --exact-depth 1 ..."',
#    )
    parser.add_argument(
        '--fd-path', 
        default=make_default_fdpath(),
        help=f'Path to "fd" utility.',
        dest='fdpath',
    )

    return parser


#####################################


def handle_args(args):
    modify_args(args)
    sanitycheck(args)


def modify_args(args):
    if args.outdir is None:
        timestring = datetime.datetime.now().isoformat(timespec='milliseconds')
        args.outdir = os.path.abspath(
            os.path.join(
                '.',
                f'{timestring}_newer_{args.newer}_older_{args.older}_size_{args.size}',
            )
        )


def sanitycheck(args):
    if os.path.exists(args.outdir):
        raise Exception(f'Specified output directory must not exist in advance.')
    if not os.path.isdir(args.topdir):
        raise Exception(f'--topdir must be a directory.')
    #if args.depth <= 0:
    #    raise Exception(f'--depth must be 1 or greater.')


#####################################


def make_outdir_tree(outdir, args):
    os.mkdir(outdir)

    args_path = os.path.join(outdir, 'args')
    with open(args_path, 'wt') as f:
        f.write(str(vars(args)))

    interim_dir = storage_monitor_common.make_interim_dir_path(outdir)
    os.mkdir(interim_dir)

    result_path = storage_monitor_common.make_resultfile_path(outdir)

    outdir_tree = {
        'result': result_path,
        'args': args_path,
        'interim_dir': interim_dir,
    }
    return outdir_tree


#def make_fd_args_components(fdpath, newer, older, size):
#    result = dict()
#    result['cmd'] = [fdpath]
#    result['options'] = list()
#    if newer is not None:
#        result['options'].extend(['--newer', newer])
#    if older is not None:
#        result['options'].extend(['--older', older])
#    if size is not None:
#        result['options'].extend(['--size', size])
#    return result
#
#
#def get_fdargs_maker(fdpath, newer, older, size):
#    def fdargs_maker(targetpath, options=dict()):
#        fdargs = list()
#        fdargs.append(fdpath)
#        if newer is not None:
#            fdargs.extend(['--newer', newer])
#        if older is not None:
#            fdargs.extend(['--older', older])
#        if size is not None:
#            fdargs.extend(['--size', size])
#        for key, val in options.items():
#            fdargs.extend([str(key), str(val)])
#        fdargs.extend(['.', targetpath])
#        return fdargs
#
#    return fdargs_maker


#def split_subproc_out(output):
#    return re.sub('\n$', '', output).split('\n')


#def get_search_targets(topdir, depth, fdargs_maker):
#    search_files = list() 
#    search_dirs = list() 
#
#    p1 = subprocess.run(
#        fdargs_maker(
#            topdir, 
#            options={
#                '--max-depth': (depth - 1), 
#                '--type': 'f',  # this only returns non-directory files
#            },
#        ),
#        text=True,
#        capture_output=True,
#    )
#    search_files.extend(split_subproc_out(p1.stdout))
#
#    p2 = subprocess.run(
#        fdargs_maker(
#            topdir, 
#            options={
#                '--exact-depth': depth,
#            },
#        ),
#        text=True,
#        capture_output=True,
#    )
#    for line in split_subproc_out(p2.stdout):
#        if os.path.isdir(line):
#            search_dirs.append(line)
#        else:
#            search_files.append(line)
#
#    return search_files, search_dirs


def make_fdargs_base(fdpath, topdir, newer=None, older=None, size=None, threads=None):
    fdargs = [
        fdpath,
        '--newer', newer,
        '--older', older,
        '--size', size,
        '--type', 'f',  # this excludes directories and symlinks
        '.', 
        topdir,
    ]

    fdargs = [fdpath]
    if newer is not None:
        fdargs.extend(['--newer', str(newer)])
    if older is not None:
        fdargs.extend(['--older', str(older)])
    if size is not None:
        fdargs.extend(['--size', str(size)])
    if threads is not None:
        fdargs.extend(['--threads', str(threads)])
    fdargs.extend(['--type', 'f', '.', topdir])

    return fdargs


def make_fdargs(args):
    return make_fdargs_base(args.fdpath, args.topdir, newer=args.newer, older=args.older, size=args.size, threads=args.threads)


def print_status(args, fdargs):
    print(f'Output directory is: {args.outdir}')
    print(f'fd arguments: {fdargs}')
    print()


######################
# fd output handlers #
######################

def check_is_text(filename):
    with open(filename, 'rt') as f:
        try:
            f.readline()
        except UnicodeDecodeError:
            return False
        else:
            return True


def get_fileinfo(filename):
    stat_result = os.stat(filename)
    lastaccess_dtobj = datetime.datetime.fromtimestamp(stat_result.st_atime)
    lastmodify_dtobj = datetime.datetime.fromtimestamp(stat_result.st_mtime)
    fileinfo = {
        'filename': filename,
        'stat_result': stat_result,
        'size_gb': round(stat_result.st_size / 1024**3, 3),
        'size_bytes': stat_result.st_size,
        'username': pwd.getpwuid(stat_result.st_uid).pw_name,
        'lastmodify_dtobj': lastmodify_dtobj,
        'lastaccess_dtobj': lastaccess_dtobj,
        'lastmodify': lastmodify_dtobj.isoformat(sep='T', timespec='milliseconds'),
        'lastaccess': lastaccess_dtobj.isoformat(sep='T', timespec='milliseconds'),
        'is_text': check_is_text(filename),
    }
    return fileinfo


def make_outfile_line(fileinfo):
    line_src = [
        str(fileinfo[key]) for key in HEADER
    ]
    return '\t'.join(line_src)


def write_result(fd_proc, result_path):
    with gzip.open(result_path, 'wt') as outfile:
        outfile.write('\t'.join(HEADER) + '\n')
        for line in fd_proc.stdout:
            line = line.strip()
            print(line, flush=True)
            fileinfo = get_fileinfo(line)
            outfile.write(make_outfile_line(fileinfo) + '\n')

    returncode = fd_proc.wait()
    if returncode != 0:
        raise Exception(f'"fd" exited with an error. stderr: {fd_proc.stderr.read()}')


def write_summary(outdir_tree):
    parsed_df = storage_monitor_common.read_resultfile(outdir_tree['result'])
    outdir = os.path.join(outdir_tree['interim_dir'], 'final')
    os.mkdir(outdir)
    storage_monitor_common.write_interim(parsed_df, outdir)


def main(args):
    handle_args(args)
    utils.rootcheck_ask_force()
    outdir_tree = make_outdir_tree(args.outdir, args)
    fdargs = make_fdargs(args)

    print_status(args, fdargs)

    fd_proc = subprocess.Popen(
        fdargs, 
        bufsize=1, 
        text=True, 
        stdout=subprocess.PIPE, 
        #stderr=subprocess.PIPE,
    )
    write_result(fd_proc, outdir_tree['result'])
    write_summary(outdir_tree)


