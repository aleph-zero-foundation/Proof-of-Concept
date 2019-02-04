from aleph.log_analyzer import LogAnalyzer
import os
import sys

def prepare_common_stats(process_stats, rep_dir):
	'''
	Write basic stats common to all processes to a file.
	'''
	# NOTE: the txps in this stat is actually much lower than in reality, because here it is measured as
	#		the total number of txs validated till the first timing unit was found, divided by the time
	#		to reach the first timing unit.
	#		If one wanted to calculate it more precisely then it should be something like
	#		(the number of txs included in one level) / (the time to build one level)

	n_stats = len(process_stats)
	rep_path = os.path.join(rep_dir, "common-stats.txt")
	with open(rep_path, "w") as rep_file:

		fields = ['latency', 'txps']
		header = ''.join( s.ljust(20) for s in ['name', 'median', 'min (proc_id)', 'max (proc_id)'])
		print(header)
		rep_file.write(header + '\n')
		for field in fields:
			process_stats.sort(key = lambda x: x[field])
			line = field.ljust(20)
			median = process_stats[n_stats//2][field]
			line += (f'{median:.3f}').ljust(20)
			min_val, min_proc = process_stats[0][field], process_stats[0]['process_id']
			line += (f'{min_val:.3f} ({min_proc})').ljust(20)
			max_val, max_proc = process_stats[-1][field], process_stats[-1]['process_id']
			line += (f'{max_val:.3f} ({max_proc})').ljust(20)
			print(line)
			rep_file.write(line + '\n')



if len(sys.argv) < 2:
	print("Need to provide arguments. Try one of:")
	print("1) Log_file_path     (In case the log file contains data from one process only)")
	print("   Ex: python log_analysis.py aleph.log")
	print("2) Log_file_path process_id           (In case the log file contains data from multiple processes)")
	print("   Ex: python log_analysis.py aleph.log 0")
	print("3) ALL Dir_with_multiple_logs Destination_dir")
	print("   Ex: python log_analysis.py ALL logs reports")
	sys.exit(0)

if len(sys.argv) == 4 and sys.argv[1] == 'ALL':

	log_dir = sys.argv[2]
	rep_dir = sys.argv[3]

	if not os.path.isdir(log_dir):
		print(f"No such directory {log_dir}.")
		sys.exit(0)

	if not os.path.isdir(rep_dir):
		print(f"No such directory {rep_dir}. Creating.")
		os.mkdir(rep_dir)

	list_logs = os.listdir(log_dir)
	# do not parse other.log etc.
	list_logs = sorted([log_file for log_file in list_logs if log_file.find("aleph") != -1])
	n_logs = len(list_logs)
	process_stats = []
	for log_name, ind in zip(list_logs, range(n_logs)):

		path = os.path.join(log_dir, log_name)
		print(f'Analyzing {path}...')
		analyzer = LogAnalyzer(path)
		if not analyzer.analyze():
			print('Failed because the log does not even contain the Process start message.')
			continue
		process_id = analyzer.process_id
		print(f"{ind}: Process' {process_id} log analyzed.\n")
		analyzer.prepare_basic_report(rep_dir)
		analyzer.prepare_report_per_process(rep_dir)

		stats = {'process_id' : process_id}
		stats['latency'] = analyzer.get_unit_latency()
		stats['txps'] = analyzer.get_txps_till_first_timing_unit()
		process_stats.append(stats)

	prepare_common_stats(process_stats, rep_dir)


if len(sys.argv) in [2,3]:
	path = sys.argv[1]
	if len(sys.argv) == 3:
		process_id = int(sys.argv[2])
	else:
		process_id = None

	analyzer = LogAnalyzer(path, process_id)
	if not analyzer.analyze():
		print('Failed because the log does not even contain the Process start message.')
		sys.exit(0)
	process_id = analyzer.process_id
	analyzer.prepare_basic_report('.')
	analyzer.prepare_report_per_process('.')
