import matplotlib.pyplot as plt
import numpy as np

from aleph.log_analyzer.log_parser import LogParser





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
        self.current_recv_sync_no = []
        self.read_process_id = process_id
        self.file_path = file_path
        self.memory_info = []
        self.start_date = None
        self.add_run_times = []
        self.process_id = None

    def set_start_date(self, date):
        '''
        Set the "genesis" date (when the process is run), if it has not been set yet.
        '''
        if self.start_date is None:
            self.start_date = date
            self.levels[0] = {'date': date}

    def handle_event(self, event):
        '''
        Takes one event as input and udates the internal state of the analyzer appropriately.
        :param dict event: event describing one line in the log.
        '''
        ev_type = event['type']

        if ev_type == 'start_process':
            self.n_processes = event['n_processes']
            self.process_id = event['process_id']
            self.set_start_date(event['date'])

        if ev_type == 'create_add':
            assert event['units'] not in self.units, "Unit hash collision?"
            self.set_start_date(event['date'])
            U = event['units']
            self.units[U] = {'created': event['date']}
            self.create_attempt_dates.append(event['date'])

        if ev_type == 'add_linear_order':
            level = event['level']
            self.levels[level]['n_units_decided'] = event['n_units']
            self.levels[level]['n_txs_ordered'] = event['n_txs']
            for U in event['units']:
                assert U in self.units, f"Unit {U} being added to linear order, but its appearance not noted."
                self.units[U]['ordered'] = event['date']

        if ev_type == 'add_done':
            for U in event['units']:
                if U not in self.units:
                    self.units[U] = {'received': [event['date']]}
                else:
                    U_dict = self.units[U]
                    assert 'created' not in U_dict, f"Unit created by {self.read_process_id} later also received from another process."
                    U_dict['received'].append(event['date'])

        # if ev_type == 'add_foreign':
        #     U = event['units']
        #     if U not in self.units:
        #         self.units[U] = {'received': [event['date']]}
        #     else:
        #         U_dict = self.units[U]
        #         assert 'created' not in U_dict, f"Unit created by {self.read_process_id} later also received from another process."
        #         U_dict['received'].append(event['date'])

        if ev_type == 'new_level':
            level = event['level']
            assert level not in self.levels, f"The same level {level} reached for the second time."
            self.levels[level] = {'date': event['date']}

        if ev_type == 'memory_usage':
            entry = {'date': event['date'], 'memory': event['memory'], 'poset_size': len(self.units)}
            self.memory_info.append(entry)

        if ev_type == 'decide_timing':
            self.levels[event['level']]['timing_decided_level'] = event['timing_decided_level']
            self.levels[event['level']]['timing_decided_date'] = event['date']

        if ev_type == 'sync_establish':
            sync_id = event['sync_id']
            if sync_id not in self.syncs:
                self.syncs[sync_id] = {}
                self.syncs[sync_id]['start_date'] = event['date']
            else:
                self.syncs[sync_id]['conn_est_time'] = diff_in_seconds(self.syncs[sync_id]['start_date'], event['date'])
                self.syncs[sync_id]['start_date'] = event['date']

        if ev_type == 'receive_units_start':
            sync_id = event['sync_id']
            self.syncs[sync_id]['target'] = event['target']

        if ev_type == 'send_units':
            sync_id = event['sync_id']
            self.syncs[sync_id]['units_sent'] = event['n_units']
            self.syncs[sync_id]['bytes_sent'] = event['n_bytes']

        if ev_type == 'receive_units':
            sync_id = event['sync_id']
            self.syncs[sync_id]['units_received'] = event['n_units']
            self.syncs[sync_id]['bytes_received'] = event['n_bytes']

        if ev_type == 'sync_success':
            sync_id = event['sync_id']
            self.syncs[sync_id]['stop_date'] = event['date']

        if ev_type == 'try_sync':
            # this is an outgoing sync
            sync_id = event['sync_id']
            assert sync_id not in self.syncs
            self.syncs[sync_id] = {}
            self.syncs[sync_id]['tried'] = True
            self.syncs[sync_id]['start_date'] = event['date']
            self.syncs[sync_id]['target'] = event['target']

            self.sync_attempt_dates.append(event['date'])

        if ev_type == 'listener_sync_no':
            self.current_recv_sync_no.append(event['n_recv_syncs'])

        if ev_type == 'add_run_time':
            avg_time = event['tot_time']/event['n_units']
            units_in_poset = len(self.units)
            # good to have the number of units as well to create a nice plot
            self.add_run_times.append((units_in_poset, avg_time))



    def analyze(self):
        '''
        Reads events from the log using the LogParser class and pulls them through handle_event.
        '''
        log_parser = LogParser(self.file_path, self.read_process_id)
        for event in log_parser.get_events():
            self.handle_event(event)

        if self.process_id is None:
            return False

        return True


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

    def get_run_time_stats(self, plot_file = None):
        '''
        Return statistics regarding the time of adding one unit to the poset.
        '''
        n_units_series = [p[0] for p in self.add_run_times]
        run_time_series = [p[1] for p in self.add_run_times]


        if plot_file is not None:
            fig, ax = plt.subplots()
            ax.plot(n_units_series , run_time_series)
            ax.set(xlabel='#units', ylabel='adding to poset time (sec)', title='Units in poset vs processing time of 1 unit.')
            fig.savefig(plot_file)

        return run_time_series


    def prepare_report_per_process(self, report_file = 'report-proc'):
        '''
        Create the file with a a summary od synchronization statistics to all the remaining processes.
        :param string report_file: the path to the file where the report should be written
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

        with open(report_file+'.txt', "w") as rep_file:
            for line in lines:
                rep_file.write(line+'\n')


    def prepare_basic_report(self, report_file = r'rep/report'):
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
        #print("level 1 decided: ", self.levels[1]['timing_decided_date'])
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
        sent_per_sync, recv_per_sync, time_per_sync, time_per_unit_ex, \
            bytes_per_unit_ex, est_conn_time, syncs_not_succ = self.get_sync_info('sync_data.png')
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
        data = self.get_memory_usage_vs_poset_size('memory.png')
        _append_stat_line(data, 'memory_MiB')

        # running time of add_unit
        data = self.get_run_time_stats('run_time.png')
        _append_stat_line(data, 'add_unit_time_s')


        #self.get_sync_info_per_process()

        with open(report_file+'.txt', "w") as rep_file:
            for line in lines:
                rep_file.write(line+'\n')






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


