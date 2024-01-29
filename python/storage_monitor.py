import sys
import argparse

import storage_monitor_run
import storage_monitor_interim


def make_parser():
    parser = argparse.ArgumentParser(
        description=(
            f'* Creates a summary table for screened files with specified criteria (e.g. modified within the last 50 days).'
            '\n'
            f'* Makes use of "fd" utility (https://github.com/sharkdp/fd).'
            '\n'
            f'* For more help, see the help message for the subcommand "run".'
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    subparsers = parser.add_subparsers(
        title='subcommands',
        dest='subcmd',
        required=True,
    )

    parser_run = subparsers.add_parser(
        'run',
        help=f'The main program',
        description=f'Makes an output file which contains information (e.g. username, size, last modification) for files with the specified criteria.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    storage_monitor_run.populate_parser(parser_run)

    parser_interim = subparsers.add_parser(
        'interim',
        help=f'Makes an interim summary from the result of "run" command.',
        description=f'Makes an interim summary from the result of "run" command. Works even before "run" command has finished yet. Useful when "run" commands takes a long time to finish.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    storage_monitor_interim.populate_parser(parser_interim)

    return parser


def main():
    parser = make_parser()
    args = parser.parse_args()
    if args.subcmd == 'run':
        storage_monitor_run.main(args)
    elif args.subcmd == 'interim':
        storage_monitor_interim.main(args)
        

if __name__ == '__main__':
    main()

