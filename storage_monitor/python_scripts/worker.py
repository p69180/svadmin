import os
import sys
import argparse
import pwd
import multiprocessing
import time
import datetime
import itertools
import gzip
import uuid

from pprint import pprint

sys.path.append('/home/users/pjh/scripts/python_genome_packages')
import julib.common


DEFAULT_MTIME_DAYS = 10
DEFAULT_SIZE_GB = 1
DEFAULT_EXCLUDED_DIRS = [
	'anaconda3', 
	'miniconda3', 
	'.cpan', 
	'python3.7', 
	'R', 
	'perl5',
	]
DEFAULT_JOB_COUNT = 50000
MAX_FILE_COUNT_PER_JOB = 100


'''
def argument_parsing():
	parser = argparse.ArgumentParser()

	parser.add_argument('--dir', required = True, type = os.path.abspath,
			help = 'Directory to search into')

	parser.add_argument('--outdir', required = True, 
			help = 'Directory where results are saved. Creates a new directory if not already present.')

	parser.add_argument('-p', required = False, default = 10, type = int, help = 'Number of jobs to run in parallel')

	parser.add_argument('--mtime', required = False, default = DEFAULT_MTIME_DAYS, type = int,
			help = f'Files whose last modification time is within <mtime> days are not counted. Default: {DEFAULT_MTIME_DAYS}')

	parser.add_argument('--size', required = False, default = DEFAULT_SIZE_GB, type = float,
			help = f'Files smaller than <size> gigabytes are not counted. Default: {DEFAULT_SIZE_GB}')

	parser.add_argument('--exclude', required = False, nargs = '*',
			default = DEFAULT_EXCLUDED_DIRS,
			help = f'Directory names to exclude from counting. Default: {DEFAULT_EXCLUDED_DIRS}')

	parser.add_argument('--job-count', dest = 'jobcount', required = False, default = DEFAULT_JOB_COUNT, type = int,
			help = f'Maximum number of Searching job  Default: {DEFAULT_JOB_COUNT}')

	args = parser.parse_args()
	args.mtime_sec = args.mtime * 3600 * 24
	args.size_bytes = args.size * (1024**3)

	return args
'''


def print_ts(s):
	print(f'[{julib.common.get_timestamp()}] {s}', flush = True, file = sys.stderr)


def modify_args(mtime_days, size_gb):
	mtime_sec = mtime_days * 3600 * 24
	size_bytes = size_gb * (1024**3)

	return mtime_sec, size_bytes


def outdir_validity_check(outdir):
	outdir_exists = julib.common.check_outdir_validity(outdir)

	return outdir_exists

			
def walkdown(excluded_dirs, target_dir, jobcount):
	def add_filenames(jobtarget_files, filenames):
		jobtarget_files[-1].extend([ os.path.join(top, x) for x in filenames ])
		if len(jobtarget_files[-1]) > MAX_FILE_COUNT_PER_JOB:
			jobtarget_files.append(list())

	def add_dirnames(walk_dirs_current, dirnames):
		walk_dirs_current.extend([ 
				os.path.join(top, x) for x in dirnames 
				if os.path.basename(x) not in excluded_dirs
				])

	jobtarget_files = list()
	jobtarget_files.append(list())
	
	walk_dirs_previous = [ target_dir ]
	while True:
		walk_dirs_current = list()

		outer_break = False
		for idx, walk_dir in enumerate(walk_dirs_previous):
			if os.path.basename(walk_dir) in excluded_dirs:
				continue

			try:
				top, dirnames, filenames = next( os.walk(walk_dir) )
			except:
				print(walk_dir)
				raise

			add_filenames(jobtarget_files, filenames)
			add_dirnames(walk_dirs_current, dirnames)

			if len(jobtarget_files) + len(walk_dirs_current) + len(walk_dirs_previous[idx+1:]) > jobcount:
				walk_dirs_current.extend( walk_dirs_previous[idx+1:] )
				outer_break = True
				break

		if outer_break:
			break

		if len(walk_dirs_current) == 0:
			break

		walk_dirs_previous = walk_dirs_current

	jobtarget_dirs = walk_dirs_current

	return jobtarget_files, jobtarget_dirs


def get_current_time():
	return time.time()


##############################################


def seconds_since_epoch_to_readable(sse):
	return datetime.datetime( *time.localtime(sse)[:6] ).isoformat(sep = '_')


def check_binary(filename):
	try:
		with open(filename, 'r', encoding = 'utf-8') as f:
			line = next(f)
	except UnicodeDecodeError:
		return True
	else:
		return False


def get_fileinfo(filename, stat):
	fileinfo = dict()
	fileinfo['filepath'] = filename
	fileinfo['filesize'] = stat.st_size
	fileinfo['filesize_GB'] = str(round( stat.st_size / (1024**3), 3 )) + 'G'
	fileinfo['mtime'] = stat.st_mtime
	fileinfo['mtime_readable'] = seconds_since_epoch_to_readable(stat.st_mtime)
	fileinfo['owner'] = pwd.getpwuid(stat.st_uid)[0]
	fileinfo['is_binary'] = check_binary(filename)

	return fileinfo


def file_filter(stat, current_time, size_bytes, mtime_sec):
	"""Excluded if False"""

	if stat.st_size < size_bytes:
		return False
	elif current_time - stat.st_mtime < mtime_sec:
		return False
	else:
		return True


##############################################


def add_fileinfo_to_result(filename, result, current_time, size_bytes, mtime_sec):
	#print_ts(filename)

	stat = os.lstat(filename)
	if file_filter(stat, current_time, size_bytes, mtime_sec):
		print_ts(filename)
		fileinfo = get_fileinfo(filename, stat)
		result.append(fileinfo)


def job_files(filename_list, current_time, size_bytes, mtime_sec, procs = None):
	if procs is not None:
		pid = uuid.uuid4().hex
		procs[pid] = True
		
	result = list()
	#NR = 0
	for filename in filename_list:
		#NR += 1
		#if NR % 50 == 0:
		#	print_ts(filename)
		add_fileinfo_to_result(filename, result, current_time, size_bytes, mtime_sec)

	if procs is not None:
		procs[pid] = False

	return result


def job_dirs(dirname, current_time, excluded_dirs, size_bytes, mtime_sec, procs = None):
	if procs is not None:
		pid = uuid.uuid4().hex
		procs[pid] = True

	result = list()
	#NR = 0
	for top, dirs, files in os.walk(dirname):
		if os.path.basename(top) in excluded_dirs:
			continue
		for filename in [ os.path.join(top, x) for x in files ]:
			#NR += 1
			#if NR % 1000 == 0:
				#print_ts(filename)
			add_fileinfo_to_result(filename, result, current_time, size_bytes, mtime_sec)

	if procs is not None:
		procs[pid] = False

	return result


def run_jobs(
		jobtarget_files, jobtarget_dirs, current_time, 
		parallel,
		mtime_sec,
		size_bytes,
		excluded_dirs,
		):
	async_result_list = list()
	result_list = list()

	with multiprocessing.Pool(parallel) as p, multiprocessing.Manager() as m:
		procs = m.dict()

		for filename_list in jobtarget_files:
			async_result_list.append( 
					p.apply_async(
						job_files, 
						(filename_list, current_time, size_bytes, mtime_sec, procs)
						) 
					)
		for dirname in jobtarget_dirs:
			async_result_list.append( 
					p.apply_async(
						job_dirs, 
						(dirname, current_time, excluded_dirs, size_bytes, mtime_sec, procs)
						) 
					)

		while True:
			time.sleep(5)
			print_ts(f'Number of running jobs : {sum(procs.values())}')
			if all([ x.ready() for x in async_result_list ]):
				break
			else:
				continue

	for async_result in async_result_list:
		result_list.append(async_result.get())

	return result_list


###########################################################


def get_result_single_dir(
		target_dir,
		parallel = 10,
		mtime_days = DEFAULT_MTIME_DAYS,
		size_gb = DEFAULT_SIZE_GB,
		excluded_dirs = DEFAULT_EXCLUDED_DIRS,
		jobcount = DEFAULT_JOB_COUNT,
		):
	mtime_sec, size_bytes = modify_args(mtime_days, size_gb)

	jobtarget_files, jobtarget_dirs = walkdown(excluded_dirs, target_dir, jobcount)

	current_time = get_current_time()
	result_list = run_jobs(
		jobtarget_files, jobtarget_dirs, current_time, 
		parallel,
		mtime_sec,
		size_bytes,
		excluded_dirs,
		)
	fileinfo_list = list(itertools.chain.from_iterable(result_list))

	return fileinfo_list


def get_results_by_owner(fileinfo_list):
	print_ts('Sorting results')

	result_byowner = dict()
	for fileinfo in fileinfo_list:
		if fileinfo['owner'] not in result_byowner:
			result_byowner[fileinfo['owner']] = list()
		result_byowner[ fileinfo['owner'] ].append(fileinfo)

	for val in result_byowner.values():
		val.sort(key = lambda x: x['filepath'])

	return result_byowner


def write_result(result_byowner, outdir, size_gb, mtime_days):
	print_ts('Writing results')

	if not outdir_validity_check(outdir):
		os.mkdir(outdir)

	header = ['filepath', 'owner', 'filesize_GB', 'is_binary', 'mtime_readable']
	for owner, fileinfos in result_byowner.items():
		basename = f'{owner}.size_gt_{size_gb}GB_mtime_gt_{mtime_days}.tsv.gz'
		with gzip.open(os.path.join(outdir, basename), 'wt') as out_file:
			out_file.write('\t'.join(header) + '\n')
			for fileinfo in fileinfos:
				out_file.write('\t'.join(str(fileinfo[x]) for x in header) + '\n')
