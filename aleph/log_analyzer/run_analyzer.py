'''
    This is a Proof-of-Concept implementation of Aleph Zero consensus protocol.
    Copyright (C) 2019 Aleph Zero Team
    
    This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    
    You should have received a copy of the GNU General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>.
'''

from aleph.log_analyzer import LogAnalyzer
import os
import sys

def prepare_common_stats(process_stats, rep_dir):
    '''
    Write basic stats common to all processes to a file.
    '''
    # NOTE: the txps in this stat is calculated as the ratio:
    #            (total n of txs validated by timing units up to lvl L) / (time to find the timing unit at lvl L)
    # where L is the highest level for which a timing unit has been established

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


def print_help():
    print(  "Use one of:\n"
            "1) python run_analyzer.py ALL logs_dir reports_dir\n"
            "       Analyzes all logs in logs_dir and writes report to reports_dir\n"
            "       If reports_dir is not provided it uses reports_dir = logs_dir\n"
            "2) python run_analyzer.py log_file [process_id]\n"
            "       Analyzes the log_file using process_id (optional)\n"
            "       Providing process_id is mandatory if the log is shared by multiple processes\n"
        )


def analyze_one_log():
    path = sys.argv[1]
    if len(sys.argv) == 3:
        process_id = int(sys.argv[2])
    else:
        print('No process id provided -- assuming that the log comes from one process only.')
        process_id = None

    analyzer = LogAnalyzer(path, process_id, generate_plots = False)
    if not analyzer.analyze():
        print('Failed because the log does not even contain the Process start message.')
        sys.exit(0)
    process_id = analyzer.process_id
    analyzer.prepare_basic_report('.')
    analyzer.prepare_report_per_process('.')


def analyze_all_dir():
    log_dir = sys.argv[2]
    if len(sys.argv) > 3:
        rep_dir = sys.argv[3]
        same_dir = False
    else:
        rep_dir = log_dir
        same_dir = True

    if not os.path.isdir(log_dir):
        print(f"No such directory {log_dir}.")
        sys.exit(0)

    print("Entering.", log_dir)

    if not os.path.isdir(rep_dir):
        print(f"No such directory {rep_dir}. Creating.")
        os.makedirs(rep_dir, exist_ok=True)

    if same_dir and os.path.isdir(os.path.join(rep_dir, 'txt-basic')):
        print("Already analyzed. Skipping.")
        return

    list_logs = os.listdir(log_dir)
    # do not parse other.log etc.
    list_logs = sorted([log_file for log_file in list_logs if os.path.basename(log_file).find("aleph") != -1])
    process_stats = []
    for ind, log_name in enumerate(list_logs):

        path = os.path.join(log_dir, log_name)
        print(f'Analyzing {path}...')
        if ind == 0:
            generate_plots = True
            print('Will generate plots only for this log file.')
        else:
            generate_plots = False

        analyzer = LogAnalyzer(path, generate_plots = generate_plots)
        if not analyzer.analyze():
            print('Failed because the log does not even contain the Process start message.')
            continue
        process_id = analyzer.process_id
        print(f"{ind}: Process' {process_id} log analyzed.\n")
        analyzer.prepare_basic_report(rep_dir)
        analyzer.prepare_report_per_process(rep_dir)

        stats = {'process_id' : process_id}
        stats['latency'] = analyzer.get_unit_latency()
        stats['txps'] = analyzer.get_txps_till_last_timing_unit()
        process_stats.append(stats)

    prepare_common_stats(process_stats, rep_dir)



def parse_args_and_run():
    if len(sys.argv) >= 3 and sys.argv[1] == 'ALL':
        analyze_all_dir()
    elif len(sys.argv) in [2,3]:
        analyze_one_log()
    else:
        print_help()


if __name__ == '__main__':
    parse_args_and_run()

