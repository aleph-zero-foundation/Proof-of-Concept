from aleph.log_analyzer import LogAnalyzer

# the below file must exist!
path = r'aleph.log'
analyzer = LogAnalyzer(path, process_id = 0)


#path = r'results/52-77-251-76-aleph.log'
#analyzer = LogAnalyzer(path)
analyzer.prepare_basic_report()