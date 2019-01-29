from aleph.log_analyzer import LogAnalyzer
import os

# the below file must exist!
#path = r'aleph.log'
#analyzer = LogAnalyzer(path, process_id = 0)


#path = r'results/52-77-251-76-aleph.log'
#path = r'results/13-231-122-167-aleph.log'


'''
for log_name in os.listdir('results'):
	path = r'results/' + log_name
	analyzer = LogAnalyzer(path)

	analyzer.prepare_basic_report()
	process_id = analyzer.process_id
	print(f'Process {process_id} report created')
'''


path = r'results/3-0-148-73-aleph.log'
#path = r'results/18-144-56-18-aleph.log'
#path = r'results/13-211-22-137-aleph.log'
#path = r'results/54-173-207-201-aleph.log'
analyzer = LogAnalyzer(path)
analyzer.analyze()
process_id = analyzer.process_id
analyzer.prepare_basic_report(f'report-{process_id:d}')
analyzer.prepare_report_per_process(f'proc-report-{process_id:d}')
print(process_id)