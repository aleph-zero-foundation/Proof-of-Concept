from aleph.log_analyzer import LogAnalyzer
import os
import sys



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
	n_logs = len(list_logs)
	for log_name, ind in zip(list_logs, range(n_logs)):
		path = os.path.join(log_dir, log_name)
		print(f'Analyzing {path}...')
		analyzer = LogAnalyzer(path)
		if not analyzer.analyze():
			print('Failed because the log does not even contain the Process start message.')
			continue
		process_id = analyzer.process_id
		print(f"{ind}: Process' {process_id} log analyzed.")
		#basic_report_file_name = os.path.join(rep_dir,f"basic-report-{process_id:d}")
		analyzer.prepare_basic_report(rep_dir)
		#sync_report_file_name = os.path.join(rep_dir,f"sync-report-{process_id:d}")
		analyzer.prepare_report_per_process(rep_dir)


		#print(f"Reports saved to {basic_report_file_name}.txt and {sync_report_file_name}.txt")
		print('')

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
	#	basic_report_file_name = f'report-{process_id:d}'
	analyzer.prepare_basic_report('.')
	#print(f'Created {basic_report_file_name}.txt')
	#sync_report_file_name = f'proc-report-{process_id:d}'
	analyzer.prepare_report_per_process('.')
	#print(f'Created {sync_report_file_name}.txt')
	print('')
	#print(process_id)