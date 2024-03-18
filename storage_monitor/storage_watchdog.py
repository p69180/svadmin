import pprint
import os
import argparse
import gzip
import operator
import datetime
import subprocess
import pathlib
import re
import multiprocessing
import json


SUBCMD_RUN = 'run'
SUBCMD_INTERIM = 'interim'
OUTFILE_COLUMNS = [
    "filename", 
    "filesize", 
    "filesize_GB", 
    "username", 
    "lastaccess", 
    "lastaccess_sec", 
    "lastmodify", 
    "lastmodify_sec", 
    "filetype", 
    "encoding",
]
FILE_OUTPUT_PAT = re.compile(r'(.+); charset=(.+)')
SLASH_REPL = '!@%'


def argument_parsing():

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(
        required=True,
        dest='subcmd',
    )

    # subcommand "run"
    parser_run = subparsers.add_parser(
        SUBCMD_RUN, 
        help=f'Begin a screening session.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser_run.add_argument(
        '--search-topdir', 
        default='/home/users',
        required=False,
        help=f'Top directory to begin search from.',
    )
    parser_run.add_argument(
        '--outdir-topdir', 
        default=os.getcwd(),
        required=False,
        help=f'Parent directory under which output directory will be created. Basename of the output directory is like <date>_<runtime option> under the directory specified by this argument.',
    )
    parser_run.add_argument(
        '--full-outdir', 
        default=None,
        required=False,
        help=f'Full path of output directory. Overrides effect of --outdir option.',
    )
    parser_run.add_argument(
        '--nproc', 
        default=10,
        type=int,
        required=False,
        help=f'Number of parallelization.',
    )
    parser_run.add_argument(
        '--mtime', 
        default=None,
        required=False,
        help=f'The value given to "-mtime" option of "find" command.',
    )
    parser_run.add_argument(
        '--size', 
        default=None,
        required=False,
        help=f'The value given to "-size" option of "find" command.',
    )
    parser_run.add_argument(
        '--depth', 
        default=1,
        type=int,
        required=False,
        help=f'Result files are created for each N-depth file from <search-topdir>. Must be a non-negative integer. When depth is 0, only one result file for <search-topdir> will be created. When depth is 1, result files will be created for each output of "find <search-topdir> -mindepth 1 -maxdepth 1".',
    )

    # subcommand "interim"
    parser_interim = subparsers.add_parser(
        SUBCMD_INTERIM, 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help=f'Make an interim summary from results of a session not yet finished.',
    )
    parser_interim.add_argument(
        'outdir', 
        help=f'Output directory of a screening session. It must contain a subdirectory "tmpfiles".',
    )

    # parse_args
    args = parser.parse_args()

    return args


###########################
# main running subcommand #
###########################

def check_root():
    return os.geteuid() == 0


def rootwarn():
    if not check_root():
        print(f'Only root user can run this.')
        exit(1)
    

def make_outdir_path(args):
    if args.full_outdir is None:
        datestr = datetime.datetime.now().isoformat()
        prefix = 'storagemonitor_'
        outdir_bname = f'storagemonitor_{datestr}_mtime_{args.mtime}_size_{args.size}'
        outdir_path = os.path.join(args.outdir_topdir, outdir_bname)
    else:
        outdir_path = args.full_outdir

    #os.makedirs(outdir_path)
    #print(f'Output directory: {outdir_path}')

    assert not os.path.exists(outdir_path)

    return outdir_path


def get_reldepth(target, start):
    relpath = os.path.relpath(target, start)
    if relpath == '.':
        return 0
    else:
        relpath_sp = relpath.split('/')
        assert '..' not in relpath_sp, f'".." in the relative path: target={target}, start={start}, relpath={relpath}'
        return len(relpath_sp)


def get_search_targets(search_topdir, depth):
    search_target_list = list()
    search_topdir = os.path.abspath(search_topdir)
    subproc_find = subprocess.Popen(
        [
            'find', 
            search_topdir, 
            '-maxdepth', str(depth), 
            '(', '-type', 'f', '-o', '-type', 'd', ')',
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    for subpath in subproc_find.stdout:
        subpath = subpath.replace('\n', '')
        assert not os.path.islink(subpath)
        assert os.path.exists(subpath)
        reldepth = get_reldepth(subpath, search_topdir)
        if reldepth == depth:
            search_target_list.append(subpath)
        elif reldepth < depth:
            if os.path.isfile(subpath):
                search_target_list.append(subpath)
        else:
            raise Exception(f'Calculated relative depth is greater than maxdepth: subpath={subpath}, search_topdir={search_topdir}')

    return search_target_list


def make_outdirs(outdir_path, args):
    outdir_tree = dict()

    outdir_tree['top'] = outdir_path
    os.makedirs(outdir_tree['top'])

    outdir_tree['logs'] = os.path.join(outdir_path, 'logs')
    os.makedirs(outdir_tree['logs'])

    outdir_tree['tmpfiles'] = os.path.join(outdir_path, 'tmpfiles')
    os.makedirs(outdir_tree['tmpfiles'])

    outdir_tree['args'] = os.path.join(outdir_path, 'args.json')
    with open(outdir_tree['args'], 'wt') as outfile:
        json.dump(vars(args), outfile)

    return outdir_tree


def check_excl_file(filepath):
    return any(
        re.search(r'anaconda|miniconda', x) for x in
        str(pathlib.PosixPath(filepath)).split('/')
    )


def handle_find_line(line):
    linesp = line.replace('\n', '').split('\t')
    assert len(linesp) == 8
    linedict = dict()
    linedict['filename'] = linesp[0]

    if check_excl_file(linedict['filename']):
        return None

    linedict['filesize'] = int(linesp[1])
    linedict['filesize_GB'] = f'{linedict["filesize"] / 1024 ** 3:.4}GB'
    linedict['username'] = linesp[2]
    linedict['lastaccess'] = linesp[3]
    linedict['lastaccess_sec'] = linesp[4]
    linedict['lastmodify'] = linesp[5]
    linedict['lastmodify_sec'] = linesp[6]

    lastcol_mat = FILE_OUTPUT_PAT.fullmatch(linesp[7])
    linedict['filetype'] = lastcol_mat.group(1)
    linedict['encoding'] = lastcol_mat.group(2)

    return linedict


def search_target_job(search_target, outdir_tree, args, repl=SLASH_REPL):
    norm_search_target = str(pathlib.PosixPath(search_target))  
        # normalization of path (e.g. /home//users/ -> /home/users)
    tmpfile_basename = re.sub('/', repl, norm_search_target) + '.tsv.gz'
    tmpfile_path = os.path.join(outdir_tree['tmpfiles'], tmpfile_basename)

    find_args = [
        'find', 
        search_target, 
        '-type', 'f', 
    ]
    if args.size is not None:
        find_args.extend(['-size', args.size])
    if args.mtime is not None:
        find_args.extend(['-mtime', args.mtime])
    find_args.extend(
        [
            '-printf', r'%p\t%s\t%u\t%Ac\t%A@\t%Tc\t%T@\t',
            '-exec', 'file', '-bi', '{}', ';',
        ],
    )
    subproc_find = subprocess.Popen(
        find_args,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    with gzip.open(tmpfile_path, 'wt') as outfile:
        outfile.write('\t'.join(OUTFILE_COLUMNS) + '\n')
        for line in subproc_find.stdout:
            linedict = handle_find_line(line)
            if linedict is None:
                continue
            outfile.write(
                '\t'.join(str(linedict[key]) for key in OUTFILE_COLUMNS) 
                + '\n'
            )


def run_search_session(args):
    rootwarn()
    assert args.subcmd == SUBCMD_RUN
    outdir_path = make_outdir_path(args)
    outdir_tree = make_outdirs(outdir_path, args)
    search_target_list = get_search_targets(args.search_topdir, args.depth)
    with multiprocessing.Pool(args.nproc) as pool:
        pool.starmap(
            search_target_job,
            (
                (search_target, outdir_tree, args) 
                for search_target in search_target_list
            )
        )


##########################
# making interim summary #
##########################

def parse_tmpfile(filepath, resultdict):
    with gzip.open(filepath, 'rt') as infile:
        try:
            _ = next(infile)
        except StopIteration:
            return 
        except UnicodeDecodeError:
            return
        else:
            try:
                for line in infile:
                    linedict = dict(zip(OUTFILE_COLUMNS, line.replace('\n', '').split('\t')))
                    if len(linedict) != len(OUTFILE_COLUMNS):
                        continue
                    linedict['filesize'] = int(linedict['filesize'])
                    resultdict.setdefault(linedict['username'], list())
                    resultdict[linedict['username']].append(linedict)
            except UnicodeDecodeError:
                return


def make_resultdict(tmpfile_dir):
    resultdict = dict()
    for fname in os.listdir(tmpfile_dir):
        print(f'Reading from temporary file: {fname}', flush=True)
        filepath = os.path.join(tmpfile_dir, fname)
        parse_tmpfile(filepath, resultdict)

    return resultdict


def write_result(resultdict, interim_dir):
    for user, sublist in resultdict.items():
        outfile_path = os.path.join(interim_dir, f'{user}.tsv.gz')
        with gzip.open(outfile_path, 'wt') as outfile:
            outfile.write('\t'.join(OUTFILE_COLUMNS) + '\n')
            for linedict in sorted(sublist, key=(lambda x: x['filesize']), reverse=True):
                outfile.write(
                    '\t'.join(str(linedict[key]) for key in OUTFILE_COLUMNS) + '\n'
                )


def write_sizebyuser(resultdict, interim_dir):
    outfile_path = os.path.join(interim_dir, f'_SIZE_BY_USER.tsv.gz')
    assert not os.path.exists(outfile_path)

    sizebyuser = {
        key: sum(x['filesize'] for x in val) 
        for key, val in resultdict.items()
    }
    with gzip.open(outfile_path, 'wt') as outfile:
        for user, sizesum in sorted(sizebyuser.items(), key=operator.itemgetter(1), reverse=True):
            sizesum_gb = round(sizesum / 1024**3, 3)
            outfile.write(f'{user}\t{sizesum_gb} GB\n')


def make_interim_summary(outdir):
    # parse tmpfiles
    tmpfile_dir = os.path.join(outdir, 'tmpfiles')
    assert os.path.exists(tmpfile_dir)
    resultdict = make_resultdict(tmpfile_dir)

    # write result
    interim_topdir = os.path.join(outdir, 'interim_results')
    os.makedirs(interim_topdir, exist_ok=True)
    interim_dir = os.path.join(interim_topdir, datetime.datetime.now().isoformat())
    os.makedirs(interim_dir, exist_ok=True)
    print(f'Writing interim summary to {interim_dir}')
    write_result(resultdict, interim_dir)
    write_sizebyuser(resultdict, interim_dir)


########
# main #
########

def main():
    args = argument_parsing()
    if args.subcmd == SUBCMD_INTERIM:
        make_interim_summary(args.outdir)
    elif args.subcmd == SUBCMD_RUN:
        run_search_session(args)
        

if __name__ == '__main__':
    main()



