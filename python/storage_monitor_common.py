import os

import pandas as pd


INTERIM_BASENAME = 'interim_summaries'


def make_interim_dir_path(outdir):
    return os.path.join(outdir, INTERIM_BASENAME)


def make_resultfile_path(outdir):
    return os.path.join(outdir, 'result.tsv')


def read_resultfile(result_path):
    parsed_df = pd.read_csv(
        result_path, header=0, sep='\t', on_bad_lines='skip',
        dtype={'size_gb': float, 'size_bytes': int, 'is_text': bool},
    )
    return parsed_df


def write_interim(parsed_df, outdir):
    grouped_df = dict((user, subdf) for (user, subdf) in parsed_df.groupby('username'))
    for user, subdf in parsed_df.groupby('username'):
        outfile_path = os.path.join(outdir, f'{user}.tsv.gz')
        subdf = subdf.sort_values('size_gb', ascending=False)
        subdf.to_csv(outfile_path, index=False, header=True, sep='\t')

