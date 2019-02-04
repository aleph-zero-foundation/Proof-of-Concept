import matplotlib.pyplot as plt
import numpy as np
import os
from datetime import datetime
import parse






class LogAnalyzer:
    '''
    A class for producing statistics about the protocol execution given logged events.
    The events (in the form of dictionaries) are created by the LogParser class.
    It also creates diagrams for certain statistics.
    :param int process_id: Optional parameter: if specified only the log messages corresponding to this processes
                           will be considered. If None, all log messages will be considered, yet in this case
                           it is assumed that only one process wrote logs to this file.
    '''
    def __init__(self, file_path, process_id = None):
        self.units = {}
        self.syncs = {}
        self.levels = {}
        self.sync_attempt_dates = []
        self.create_attempt_dates = []

        self.create_times = []

        self.current_recv_sync_no = []
        self.read_process_id = process_id
        self.file_path = file_path
        self.memory_info = []
        self.start_date = None
        self.add_run_times = []
        self.process_id = None



        # initialize a bunch of parsing patterns
        # doing parse.compile() beforehand once is to speed-up the parsing
        self.msg_pattern = parse.compile("[{date}] [{msg_level}] [{name}] {msg} [{file_line}]")
        self.split_on_bar = parse.compile("{left}|{right}")

        self.pattern_create = parse.compile("Created a new unit <{unit}>")
        self.pattern_memory = parse.compile("{usage:f} MiB")
        self.pattern_level = parse.compile("Level {level:d} reached")
        self.pattern_add_line = parse.compile("At lvl {timing_level:d} added {n_units:d} units and {n_txs:d} txs to the linear order {unit_list}")
        self.pattern_decide_timing = parse.compile("Timing unit for lvl {level:d} decided at lvl + {plus_level:d}")
        self.pattern_sync_establish = parse.compile("Established connection to {process_id:d}")
        self.pattern_listener_succ = parse.compile("Syncing with {process_id:d} succesful")
        self.pattern_receive_units_done = parse.compile("Received {n_bytes:d} bytes and {n_units:d} units")
        self.pattern_send_units_sent = parse.compile("Sent {n_units:d} units and {n_bytes:d} bytes to {ex_id:d}")
        self.pattern_try_sync = parse.compile("Establishing connection to {target:d}")
        self.pattern_listener_sync_no = parse.compile("Number of syncs is {n_recv_syncs:d}")
        self.pattern_add_run_time = parse.compile("Added {n_units:d} in {tot_time:f} sec")
        self.pattern_start_process = parse.compile("Starting a new process in committee of size {n_processes:d}")
        self.pattern_receive_units_start = parse.compile("Receiving units from {target:d}")
        self.pattern_add_done = parse.compile("units from {target:d} were added succesfully {unit_list}")
        self.pattern_timer = parse.compile("{timer_name} took {time_spent:f} s")


        # create the mapping between event types and the functions used for parsing this types of events

        self.parse_mapping = {
            'create_add' : self.parse_create,
            'memory_usage' : self.parse_mem_usg,
            'add_linear_order' : self.parse_add_lin_order,
            'new_level' : self.parse_new_level,
            'decide_timing' : self.parse_decide_timing,
            'sync_establish' : self.parse_establish,
            'listener_establish' : self.parse_establish,
            'sync_done' : self.parse_listener_succ,
            'listener_succ' : self.parse_listener_succ,
            'receive_units_done_listener' : self.parse_receive_units_done,
            'receive_units_done_sync' : self.parse_receive_units_done,
            'send_units_sent_listener' : self.parse_send_units_sent,
            'send_units_sent_sync' : self.parse_send_units_sent,
            'sync_establish_try' : self.parse_try_sync,
            'listener_sync_no' : self.parse_listener_sync_no,
            'add_run_time' : self.parse_add_run_time,
            'start_process' : self.parse_start_process,
            'receive_units_start_listener': self.parse_receive_units_start,
            'receive_untis_start_sync' : self.parse_receive_units_start,
            'add_received_done_listener' : self.parse_add_received_done,
            'add_received_done_sync' : self.parse_add_received_done,
            'timer' : self.parse_timer,
            }

    # Functions for parsing specific types of log messages. Typically one function per log lessage type.
    # Although some functions support more of them at the same time
    # Each of these functions takes the same set of parameters, being:
    # -- ev_params: parameters of the log event, typically process_id or/and sync_id
    # -- msg_body: the remaining part of the log message that carries information specific to this event type
    # -- event: this is the partial result of parsing of the current line, it has a date field etc.



    # ----------- START PARSING FUNCTIONS ---------------

    def parse_create(self,  ev_params, msg_body, event):
        parsed = self.pattern_create.parse(msg_body)
        U = parsed['unit']
        assert U not in self.units, "Unit hash collision?"
        if self.levels == {} :
            self.levels[0] = {'date' : event['date']}
        self.units[U] = {'created': event['date']}
        self.create_attempt_dates.append(event['date'])

    def parse_timer(self,  ev_params, msg_body, event):
        parsed = self.pattern_timer.parse(msg_body)
        timer_name = parsed['timer_name']
        time_spent = parsed['time_spent']
        if len(ev_params) > 1:
            sync_id = int(ev_params[1])
            self.syncs[sync_id]['t_' + timer_name] = time_spent
        else:
            if timer_name == "create_unit":
                self.create_times.append(time_spent)
            else:
            # this is 'linear_order_L' where L is the level
                level = parse.parse("linear_order_{level:d}", timer_name)['level']
                self.levels[level]['t_lin_order'] = time_spent


    def parse_mem_usg(self, ev_params, msg_body, event):
        parsed = self.pattern_memory.parse(msg_body)
        entry = {'date': event['date'], 'memory': parsed['usage'], 'poset_size': len(self.units)}
        self.memory_info.append(entry)

    def parse_new_level(self, ev_params, msg_body, event):
        parsed = self.pattern_level.parse(msg_body)

        level = parsed['level']
        assert level not in self.levels, f"The same level {level} reached for the second time."
        self.levels[level] = {'date': event['date']}

    def parse_start_process(self, ev_params, msg_body, event):
        parsed = self.pattern_start_process.parse(msg_body)

        self.n_processes = parsed['n_processes']
        self.process_id = event['process_id']
        self.start_date = event['date']

    def parse_add_received_done(self, ev_params, msg_body, event):
        # need to add '  ' at the end of msg_body so that empty unit list gets parsed correctly
        # this is because empty string is not a correct match while whitespace is fine
        parsed = self.pattern_add_done.parse(msg_body+'  ')
        units = parse_unit_list(parsed['unit_list'])
        sync_id = int(ev_params[1])

        for U in units:
            if U not in self.units:
                self.units[U] = {'received': [event['date']]}
            else:
                U_dict = self.units[U]
                assert 'created' not in U_dict, f"Unit created by {self.read_process_id} later also received from another process."
                U_dict['received'].append(event['date'])

    def parse_receive_units_start(self, ev_params, msg_body, event):
        parsed = self.pattern_receive_units_start.parse(msg_body)
        sync_id = int(ev_params[1])
        self.syncs[sync_id]['target'] = parsed['target']

    def parse_add_run_time(self, ev_params, msg_body, event):
        parsed = self.pattern_add_run_time.parse(msg_body)
        avg_time = parsed['tot_time']/parsed['n_units']
        units_in_poset = len(self.units)
        # good to have the number of units as well to create a nice plot
        self.add_run_times.append((units_in_poset, avg_time))

    def parse_listener_succ(self, ev_params, msg_body, event):
        parsed = self.pattern_listener_succ.parse(msg_body)
        sync_id = int(ev_params[1])
        self.syncs[sync_id]['stop_date'] = event['date']

    def parse_try_sync(self, ev_params, msg_body, event):
        parsed = self.pattern_try_sync.parse(msg_body)
        sync_id = int(ev_params[1])
        target = parsed['target']

        assert sync_id not in self.syncs
        self.syncs[sync_id] = {}
        self.syncs[sync_id]['tried'] = True
        self.syncs[sync_id]['start_date'] = event['date']
        self.syncs[sync_id]['target'] = target

        self.sync_attempt_dates.append(event['date'])

    def parse_listener_sync_no(self, ev_params, msg_body, event):
        parsed = self.pattern_listener_sync_no.parse(msg_body)
        self.current_recv_sync_no.append(parsed['n_recv_syncs'])

    def parse_establish(self, ev_params, msg_body, event):
        sync_id = int(ev_params[1])

        if sync_id not in self.syncs:
            self.syncs[sync_id] = {}
            self.syncs[sync_id]['start_date'] = event['date']
        else:
            self.syncs[sync_id]['conn_est_time'] = diff_in_seconds(self.syncs[sync_id]['start_date'], event['date'])
            self.syncs[sync_id]['start_date'] = event['date']

    def parse_add_lin_order(self, ev_params, msg_body, event):
        parsed = self.pattern_add_line.parse(msg_body)
        units = parse_unit_list(parsed['unit_list'])
        level = parsed['timing_level']
        assert parsed['n_units'] == len(units)

        self.levels[level]['n_units_decided'] = parsed['n_units']
        self.levels[level]['n_txs_ordered'] = parsed['n_txs']

        for U in units:
            assert U in self.units, f"Unit {U} being added to linear order, but its appearance not noted."
            self.units[U]['ordered'] = event['date']

    def parse_decide_timing(self, ev_params, msg_body, event):
        parsed = self.pattern_decide_timing.parse(msg_body)
        level = parsed['level']
        timing_decided_level = parsed['level'] + parsed['plus_level']

        self.levels[level]['timing_decided_level'] = timing_decided_level
        self.levels[level]['timing_decided_date'] = event['date']

    def parse_receive_units_done(self, ev_params, msg_body, event):
        parsed = self.pattern_receive_units_done.parse(msg_body)

        sync_id = int(ev_params[1])
        self.syncs[sync_id]['units_received'] = parsed['n_units']
        self.syncs[sync_id]['bytes_received'] = parsed['n_bytes']



    def parse_send_units_sent(self, ev_params, msg_body, event):
        parsed = self.pattern_send_units_sent.parse(msg_body)

        sync_id = int(ev_params[1])
        self.syncs[sync_id]['units_sent'] = parsed['n_units']
        self.syncs[sync_id]['bytes_sent'] = parsed['n_bytes']

    # ----------- END PARSING FUNCTIONS ---------------



    def parse_and_handle_log_line(self, line):
        '''
        Given a line from the log, parse it and update the internal state of the analyzer.
        :param string line: one line from the log
        :returns: True or False depending on whether parsing was succesful
        '''
        # use a parse pattern to extract the date, the logger name etc. from the line
        parsed_line =  self.msg_pattern.parse(line)
        if parsed_line is None:
            print('Line not parsed:')
            print(line)
            return False

        if parsed_line['msg_level'] == 'ERROR':
            print("Encountered ERROR line in the log:  ", line)
            return False
        #assert parsed_line is not None
        # create the event to be the dict of parsed data from the line
        # some of the fields might be then overwritten or removed and, of course, added
        event = parsed_line.named

        msg = event['msg']
        # the bar '|' splits the message into "msg_type + basic parameters" and "msg_body"
        split_msg = self.split_on_bar.parse(msg)
        if split_msg is None:
            # this happens when some log message is not formatted using "|"
            # it means we should skip it
            return False

        event_descr, msg_body = split_msg['left'].strip(), split_msg['right'].strip()
        ev_tokens = get_tokens(event_descr)

        # this is the "event type"
        ev_type = ev_tokens[0]


        if ev_type in self.parse_mapping:
            # this means that we support parsing this message
            if len(ev_tokens) > 1:
                event['process_id'] = int(ev_tokens[1])

            if self.process_id is not None and event['process_id'] != self.process_id:
                return False

            # parse date
            event['date'] = datetime.strptime(event['date'], "%Y-%m-%d %H:%M:%S,%f")

            # use the mapping created in __init__ to parse the msg using the appropriate function
            self.parse_mapping[ev_type](ev_tokens[1:], msg_body, event)
        else:
            # this event type is not supported yet... but we don't even need to support all of them
            return False

        return True



    def analyze(self):
        '''
        Reads lines from the log, parses them and updates the analyzer's state.
        '''
        with open(self.file_path, "r") as log_file:
            for line in log_file:
                self.parse_and_handle_log_line(line.strip())

        if self.process_id is None:
            # this means that the log does not even have a "start" message
            # most likely it is empty
            return False

        return True


    def get_unit_latency(self):
        '''
        Returns the average time from creating a unit till having it linearly ordered.
        '''

        delay_list = self.get_delays_create_order()
        if delay_list == []:
            return float('inf')
        else:
            return compute_basic_stats(delay_list)['avg']


    def get_txps_till_first_timing_unit(self):
        '''
        Returns the number of transactions per second averaged from start till deciding on the timing unit at lvl 1.
        '''
        if 1 in self.levels and 'timing_decided_date' in self.levels[1]:
            # making sure the first level has been decided
            time_till_first_level_timing = diff_in_seconds(self.levels[0]['date'], self.levels[1]['timing_decided_date'])
            return self.levels[1]['n_txs_ordered']/time_till_first_level_timing
        else:
            return 0.0


    def get_cpu_times(self):
        timer_names = ['t_compress_units', 't_decompress_units', 't_unpickle_units',
                     't_verify_signatures', 't_add_units', 't_tot_sync', 't_order_level']
        cpu_time_summary = { name : [] for name in timer_names }

        total_add_unit = 0.0
        n_add_unit = 0

        for sync_id, sync_dict in self.syncs.items():
            sync_tot_time = 0.0
            for name in timer_names:
                if name in sync_dict:
                    cpu_time_summary[name].append(sync_dict[name])
                    sync_tot_time += sync_dict[name]
            if sync_tot_time > 0.0:
                cpu_time_summary['t_tot_sync'].append(sync_tot_time)

        for level, level_dict in self.levels.items():
            if 't_lin_order' in level_dict:
                cpu_time_summary['t_order_level'].append(level_dict['t_lin_order'])

        cpu_time_summary['t_create'] = self.create_times

        return cpu_time_summary

    def get_delays_create_order(self):
        '''
        Computes delays between all consecutive create_unit events.
        '''
        delay_list = []
        for U, U_dict in self.units.items():
            if 'created' in U_dict and 'ordered' in U_dict:
                diff = diff_in_seconds(U_dict['created'], U_dict['ordered'])
                delay_list.append(diff)

        return delay_list

    def get_delays_add_foreign_order(self):
        '''
        Computes delays between adding a unit to the poset (a foreign unit, i.e. not created by us)
        and having it linearly ordered by the algorithm.
        '''
        delay_list = []
        for U, U_dict in self.units.items():
            if 'received' in U_dict and 'ordered' in U_dict:
                diff = diff_in_seconds(U_dict['received'][0], U_dict['ordered'])
                delay_list.append(diff)

        return delay_list

    def get_new_level_times(self):
        '''
        Computes delays between creation times of consecutive levels.
        '''
        delay_list = []
        for level in self.levels:
            if level == 0:
                # level = 0 starts at dealing units -- nothing interesting here
                continue
            else:
                delay = diff_in_seconds(self.levels[level-1]['date'], self.levels[level]['date'])
            delay_list.append(delay)

        return delay_list

    def get_timing_decision_stats(self):
        '''
        Returns 4 lists:
        [level],
        [n_units decided at this level],
        [+levels of timing decision at this level],
        [time in sec to timing decision)
        '''
        levels = []
        n_units_per_level = []
        n_txs_per_level = []
        levels_plus_decided = []
        level_delays = []
        for level in self.levels:
            if level == 0:
                # timing is not decided on level
                continue
            else:
                if 'timing_decided_date' in self.levels[level]:
                    n_units = self.levels[level]['n_units_decided']
                    delay = diff_in_seconds(self.levels[level]['date'], self.levels[level]['timing_decided_date'])
                    level_diff = self.levels[level]['timing_decided_level'] - level
                    levels.append(level)
                    n_units_per_level.append(n_units)
                    levels_plus_decided.append(level_diff)
                    level_delays.append(delay)
                    n_txs_per_level.append(self.levels[level]['n_txs_ordered'])
        return levels, n_units_per_level, levels_plus_decided, level_delays, n_txs_per_level

    def get_sync_info(self, plot_file = None):
        '''
        Returns statistics regarding synchronizations with other processes. More precisely:
        -- units_sent_per_sync: the (list of) number of units sent to the other process in a sync
        -- units_received_per_sync: the same as above but received instead of sent
        -- time_per_sync: the (list of) durations (in sec) of syncs
        -- time_per_unit_exchanged: the (list of) times of syncs per one unit
        -- bytes_per_unit_exchanged: the (list of) number of bytes exchanged per one unit
        -- establish_connection_times: the (list of) times to establish a connection when initiating a sync
        -- syncs_not_succeeded: one int - the number of syncs that started (i.e. n_recv_sync was incremented)
                                but for some reason did not terminate succesfully
        '''
        units_sent_per_sync = []
        units_received_per_sync = []
        time_per_sync = []
        syncs_not_succeeded = 0
        time_per_unit_exchanged = []
        bytes_per_unit_exchanged = []
        establish_connection_times = []

        for sync_id, sync in self.syncs.items():
            if not 'stop_date' in sync:
                syncs_not_succeeded += 1
                continue
            time_sync = diff_in_seconds(sync['start_date'], sync['stop_date'])
            time_per_sync.append(time_sync)
            units_sent_per_sync.append(sync['units_sent'])
            units_received_per_sync.append(sync['units_received'])
            bytes_exchanged = sync['bytes_sent'] + sync['bytes_received']
            n_units_exchanged = sync['units_sent'] + sync['units_received']
            if n_units_exchanged:
                time_per_unit_exchanged.append(time_sync/n_units_exchanged)
                bytes_per_unit_exchanged.append(bytes_exchanged/n_units_exchanged)

            if 'conn_est_time' in sync:
                establish_connection_times.append(sync['conn_est_time'])


        if plot_file is not None:
            fig, ax = plt.subplots()
            units_exchanged = [s + r for (s,r) in zip(units_sent_per_sync, units_received_per_sync)]
            x_series, y_series = units_exchanged, time_per_sync
            ax.scatter(x_series, y_series, s=1)
            ax.set(xlabel='#units', ylabel='sync time (sec)', title='Units exchanged vs sync time')
            fig.savefig(plot_file, dpi=500)
            plt.close()

        return units_sent_per_sync, units_received_per_sync, time_per_sync, \
                time_per_unit_exchanged, bytes_per_unit_exchanged, \
                establish_connection_times, syncs_not_succeeded



    def get_memory_usage_vs_poset_size(self, plot_file=None, show_plot=False):
        '''
        Returns a list of memory usages (in MiB) of the python process at regular times.
        '''

        data = []
        for entry in self.memory_info:
            data.append((entry['poset_size'], entry['memory']))

        fig, ax = plt.subplots()
        x_series, y_series = [point[0] for point in data], [point[1] for point in data]
        ax.plot(x_series, y_series)
        ax.set(xlabel='#units', ylabel='usage (MiB)', title='Memory Consumption')
        if plot_file is not None:
            fig.savefig(plot_file)
        if show_plot:
            plt.show()
        plt.close()
        return [point[1] for point in data]


    def get_delay_stats(self):
        '''
        Returns statistics on the delays between two consecutive creates and sync attempts.
        '''
        create_delays = [diff_in_seconds(self.create_attempt_dates[i-1], self.create_attempt_dates[i])
                        for i in range(1,len(self.create_attempt_dates))]
        sync_delays = [diff_in_seconds(self.sync_attempt_dates[i-1], self.sync_attempt_dates[i])
                        for i in range(1,len(self.sync_attempt_dates))]


        return create_delays, sync_delays



    def prepare_report_per_process(self, dest_dir = 'reports', file_name_prefix = 'report-sync-'):
        '''
        Create the file with a a summary od synchronization statistics to all the remaining processes.
        :param string dest_dir: the path to the directory where the report file should be written
        :param string file_name_prefix: the prefix of the filename where the report should be written,
                                        process_id and the extension '.txt' is appended at the end
        WARNING: before calling this function call LogAnalyzer.analyze() first

        Meaning of the specific columns:

        - sync_fail: the total number of failed syncs (incoming and outcoming included)

        - sync_succ: the total number of succesful syncs (incoming and outcoming included)

        - avg_time: average time (per sync) spent on "talking" to a specific process

        - n_conn: the number of outcoming connection attempts to a specific process

        - conn_est_t: the average time spent on establishing connections to a specific process

        - n_conn_fail: the number of attempted (outcoming) connections that failed to establish connection
        '''

        syncs_failed = [0] * self.n_processes
        syncs_succeded = [0] * self.n_processes
        tot_time = [0.0] * self.n_processes
        conn_est_time = [0.0] * self.n_processes
        n_conn_est = [0] * self.n_processes
        n_conn_fail = [0] * self.n_processes
        for sync_id, sync in self.syncs.items():
            if 'target' not in sync:
                # this sync has no info on which process are we talking to, cannot do much
                continue
            target = sync['target']

            if 'tried' in sync and 'conn_est_time' not in sync:
                n_conn_fail[target] += 1

            if 'conn_est_time' in sync:
                n_conn_est[target] += 1
                conn_est_time[target] += sync['conn_est_time']


            if 'stop_date' not in sync:
                syncs_failed[target] += 1
                continue

            syncs_succeded[target] += 1
            tot_time[target] += diff_in_seconds(sync['start_date'], sync['stop_date'])


        fields = ['name', 'sync_fail', 'sync_succ', 'avg_time', 'n_conn', 'conn_est_t', 'n_conn_fail']
        lines = []
        lines.append(format_line(fields))

        for target in range(self.n_processes):
            if syncs_succeded[target]:
                tot_time[target] /= syncs_succeded[target]
            else:
                tot_time[target] = -1.0
            data = {'name' : f'proc_{target}'}
            data['sync_fail'] = syncs_failed[target]
            data['sync_succ'] = syncs_succeded[target]
            data['avg_time'] = tot_time[target]

            if n_conn_est[target]:
                conn_est_time[target] /= n_conn_est[target]
            else:
                conn_est_time[target] = -1.0
            data['n_conn'] = n_conn_est[target]
            data['n_conn_fail'] = n_conn_fail[target]
            data['conn_est_t'] = conn_est_time[target]

            lines.append(format_line(fields, data))

        report_file = os.path.join(dest_dir, file_name_prefix+str(self.process_id)+'.txt')

        with open(report_file, "w") as rep_file:
            for line in lines:
                rep_file.write(line+'\n')

            print(f'Report file written to {report_file}.')


    def prepare_basic_report(self, dest_dir = 'reports', file_name_prefix = 'report-basic-'):
        '''
        Create the file with a succinct summary of the data in the report_file.
        It also creates some plots of the analyzed data.
        :param string report_file: the path to the file where the report should be written
        WARNING: before calling this function call LogAnalyzer.analyze() first

        Meaning of the specific rows:
        - n_units_decision: the number of units that are added to the linear order per timing unit

        - time_decision: the time (in sec) between creating a new level and deciding on a timing unit on this level

        - decision_height: the difference in levels between the timing unit and the unit which decided it (i.e. had Delta=1)
        - n_txs_ordered: the number of unique transactions added in a batch between two timing units
                         NOTE: there might be duplicate txs counted here, between different batches

        - new_level_times: the time needed to create a new level

        - create_ord_del: the time between creating a unit and placing it in the linear order

        - add_ord_del: the same as create_ord_del, but now the unit is by another process and
                       the timer is started once the unit is received in a sync

        - units_sent_sync: the number of units sent to the other process in one sync

        - units_recv_sync: the number of units received from the other process in one sync

        - time_per_sync: the total duration of the synchronization: start when conn. established,
                         stop when all data exchanged succesfully

        - time_per_unit_ex: the average time_per_sync averaged by the number of units exchanged

        - bytes_per_unit_ex: as time_per_unit_ex but the number of bytes transmitted
                            NOTE: only the 2nd round of communication counted (i.e. exchanging heights not counted)

        - est_conn_time: time to establish connection to another process

        - sync_fail: ONE NUMBER: total number of failed synchronizations

        - create_freq: the difference in time between two consecutive create_unit

        - sync_freq: the difference in time between two consecutive synchronization attempts

        - n_recv_syncs: the number of simultaneous incoming connections (sampled when a new process is trying to connect)

        - memory_MiB: total memory used by the process

        - add_unit_time_s: average time spend on adding one unit to the poset
        '''


        lines = []
        fields = ["name", "avg", "min", "max", "n_samples"]
        lines.append(format_line(fields))

        def _append_stat_line(data, name):
            nonlocal fields, lines
            if data == []:
                # to avoid problems with empty data
                data = [-1]
            stats = compute_basic_stats(data)
            stats['name'] = name
            lines.append(format_line(fields, stats))


        # timing_decision
        levels, n_units_per_level, levels_plus_decided, level_delays, n_txs_per_level = self.get_timing_decision_stats()
        _append_stat_line(n_units_per_level, 'n_units_decision')
        _append_stat_line(level_delays, 'time_decision')
        _append_stat_line(levels_plus_decided, 'decision_height')
        _append_stat_line(n_txs_per_level, 'n_txs_ordered')

        # new level
        data = self.get_new_level_times()
        _append_stat_line(data, 'new_level_times')

        # delay between create and order
        data = self.get_delays_create_order()
        _append_stat_line(data, 'create_ord_del')

        # delay between adding a new foreign unit and order
        data = self.get_delays_add_foreign_order()
        _append_stat_line(data, 'add_ord_del')

        # info about syncs
        sync_plot_file = os.path.join(dest_dir, 'plot-sync-' + str(self.process_id) + '.png')
        sent_per_sync, recv_per_sync, time_per_sync, time_per_unit_ex, \
            bytes_per_unit_ex, est_conn_time, syncs_not_succ = self.get_sync_info(sync_plot_file)
        _append_stat_line(sent_per_sync, 'units_sent_sync')
        _append_stat_line(recv_per_sync, 'units_recv_sync')
        _append_stat_line(time_per_sync, 'time_per_sync')
        _append_stat_line(time_per_unit_ex, 'time_per_unit_ex')
        _append_stat_line(bytes_per_unit_ex, 'bytes_per_unit_ex')
        _append_stat_line(est_conn_time, 'est_conn_time')
        _append_stat_line([syncs_not_succ], 'sync_fail')

        # delay stats
        create_delays, sync_delays = self.get_delay_stats()
        _append_stat_line(create_delays, 'create_freq')
        _append_stat_line(sync_delays, 'sync_freq')

        # number of concurrent received syncs
        data = self.current_recv_sync_no
        _append_stat_line(data, 'n_recv_syncs')

        # memory
        mem_plot_file = os.path.join(dest_dir, 'plot-mem-' + str(self.process_id) + '.png')
        data = self.get_memory_usage_vs_poset_size(mem_plot_file)
        _append_stat_line(data, 'memory_MiB')

        times = self.get_cpu_times()
        _append_stat_line(times['t_compress_units'], 'time_compress')
        _append_stat_line(times['t_decompress_units'], 'time_decompress')
        _append_stat_line(times['t_unpickle_units'], 'time_unpickle')
        _append_stat_line(times['t_verify_signatures'], 'time_verify')
        _append_stat_line(times['t_add_units'], 'time_add_units')
        _append_stat_line(times['t_tot_sync'], 'time_cpu_sync')
        _append_stat_line(times['t_order_level'], 'time_order')


        #self.get_sync_info_per_process()

        report_file = os.path.join(dest_dir, file_name_prefix+str(self.process_id)+'.txt')

        with open(report_file, "w") as rep_file:
            for line in lines:
                rep_file.write(line+'\n')

            print(f'Report file written to {report_file}.')






# ------------- Helper Functions for the Log Analyzer ------------------

def diff_in_seconds(date_from, date_to):
    return (date_to-date_from).total_seconds()

def compute_basic_stats(list_of_numbers):
    '''
    Compute the basic statistics of a data set (list of numbers) and output them as a dict.
    '''
    np_array = np.array(list_of_numbers)
    summ = {}
    summ['n_samples'] = len(list_of_numbers)
    summ['avg'] = np.mean(np_array)
    #summ['stdev'] = np.std(np_array)
    summ['min'] = np.min(np_array)
    summ['max'] = np.max(np_array)

    return summ

def format_line(field_list, data = None):
    '''
    Construct one line of the report file.
    '''

    line = ''
    for field in field_list:
        if data is None:
            value = field
        else:
            value = data[field]

        if isinstance(value, float):
            entry = f"{value:.4f}"
        else:
            entry = str(value)

        just_len = 25 if field == 'name' else 12
        entry = entry.ljust(just_len)
        line += entry
    return line


# ------------------ Helper Functions for parser ------------------

def get_tokens(space_separated_string):
    return [s.strip() for s in space_separated_string.split()]

def parse_unit_list(space_separated_units):
    return [s[1:-1] for s in space_separated_units.split()]

