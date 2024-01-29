import os
import argparse
import datetime
import gzip

import storage_monitor_common


def populate_parser(parser):
    parser.add_argument(
        'target_dir', 
        help=f'"--outdir" specified by "run" command.',
    )
    return parser


#####################################


def handle_args(args):
    modify_args(args)
    sanitycheck(args)


def modify_args(args):
    pass


def sanitycheck(args):
    interim_dir = storage_monitor_common.make_interim_dir_path(args.target_dir)
    assert os.path.exists(interim_dir)


#####################################


def make_outdir(args):
    interim_dir = storage_monitor_common.make_interim_dir_path(args.target_dir)
    timestring = datetime.datetime.now().isoformat(timespec='milliseconds')
    outdir = os.path.join(interim_dir, timestring)
    os.mkdir(outdir)
    return outdir


#####################################





#####################################


#def split_byteline(byteline):
#    return byteline.replace(b'\n', b'').split(b'\t')
#
#
#def get_header(infile_path):
#    with gzip.open(infile_path, 'rb') as infile:
#        try:
#            header_line_byte = next(infile)
#        except StopIteration:
#            return None
#        else:
#            header_linesp_byte = split_byteline(header_line_byte)
#            header = [x.decode() for x in header_linesp_byte]
#            return header
#
#
#def infile_iterator(infile, header):
#    next(infile)  # skip header line
#
#    for line_byte in infile:
#        linesp_byte = split_byteline(line_byte)
#        if len(linesp_byte) != len(header):
#            continue
#
#        try:
#            linesp = [x.decode() for x in linesp_byte]
#        except UnicodeDecodeError:
#            continue
#        else:
#            linespdict = dict(zip(header, linesp))
#            yield linespdict
#
#
#def read_resultfile(result_path):
#    lines_byuser = dict()
#    header = get_header(result_path)
#    if header is None:
#        raise Exception(f'The result file is empty.')
#
#    with gzip.open(result_path, 'rb') as infile:
#        for linespdict in infile_iterator(infile, header):
#            user = linespdict['username']
#            lines_byuser.setdefault(user, list())
#            if os.path.exists(linespdict['filename']):
#                lines_byuser[user].append(linespdict)
#
#    return lines_byuser, header
#
#
#def write(lines_byuser, outdir, colnames_to_write):
#    for user in lines_byuser.keys():
#        outfile_path = os.path.join(outdir, f'{user}.tsv.gz')
#        with gzip.open(outfile_path, 'wt') as outfile:
#            outfile.write('\t'.join(colnames_to_write) + '\n')
#            for linespdict in lines_byuser[user]:
#                line = '\t'.join(linespdict[key] for key in colnames_to_write)
#                outfile.write(line + '\n')


#####################################


def main(args):
    handle_args(args)

    result_path = storage_monitor_common.make_resultfile_path(args.target_dir)
    parsed_df = storage_monitor_common.read_resultfile(result_path)

    outdir = make_outdir(args)
    storage_monitor_common.write_interim(parsed_df, outdir)

