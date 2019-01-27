from aleph.log_analyzer import LogAnalyzer

# the below file must exist!
path = r'aleph.log'
analyzer = LogAnalyzer(path, process_id = 0)
analyzer.prepare_basic_report()