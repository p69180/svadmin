#!/home/users/pjh/conda_bin/python

import argparse
import os
import itertools
from pprint import pprint

import worker


def argument_parsing():
	parser = argparse.ArgumentParser()

	parser.add_argument('--outdir', required = True, 
			help = 'Directory where results are saved. Creates a new directory if not already present.')

	parser.add_argument('--parallel', required = False, default = 10, type = int, help = 'Number of jobs to run in parallel')

	parser.add_argument('--mtime-days', dest = 'mtime_days', required = False, 
			default = worker.DEFAULT_MTIME_DAYS, type = int,
			help = f'Files whose last modification time is within <mtime> days are not counted. Default: {worker.DEFAULT_MTIME_DAYS}')

	parser.add_argument('--size-gb', dest = 'size_gb', required = False, 
			default = worker.DEFAULT_SIZE_GB, type = float,
			help = f'Files smaller than <size> gigabytes are not counted. Default: {worker.DEFAULT_SIZE_GB}')

	parser.add_argument('--job-count', dest = 'jobcount', required = False, 
			default = worker.DEFAULT_JOB_COUNT, type = int,
			help = f'Maximum number of Searching job  Default: {worker.DEFAULT_JOB_COUNT}')

	args = parser.parse_args()

	return args


def rootwarn():
	if os.getuid() != 0: # not root
		print('''
You are not a root user.
Result will be incomplete since you cannot access some of other user's files.
Enter "yes" if you want to proceed.''')

		answer = input()
		if answer != 'yes':
			sys.exit()


def get_dirs_to_run():
	dirs_to_run = [ 
		os.path.join('/home/users', x) 
		for x in next(os.walk('/home/users'))[1] 
		if x != 'changhyunnam'
	]

	return dirs_to_run


def main():
	args = argument_parsing()
	rootwarn()

	dirs_to_run = get_dirs_to_run()

	fileinfo_list_list = list()
	for target_dir in dirs_to_run:
		worker.print_ts(target_dir)
		fileinfo_list_list.append( 
				worker.get_result_single_dir(
					target_dir,
					parallel = args.parallel,
					mtime_days = args.mtime_days,
					size_gb = args.size_gb,
					jobcount = args.jobcount,
					) 
				)
	fileinfo_list = list(itertools.chain.from_iterable(fileinfo_list_list))
	result_by_owner = worker.get_results_by_owner(fileinfo_list)
	worker.write_result(result_by_owner, args.outdir, args.size_gb, args.mtime_days)


if __name__ == '__main__':
	main()
