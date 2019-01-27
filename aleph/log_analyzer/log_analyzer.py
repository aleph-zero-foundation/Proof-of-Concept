import matplotlib.pyplot as plt
import numpy as np
from aleph.log_analyzer.log_parser import LogParser







class LogAnalyzer:
    '''
    A class for producing statistics about the protocol execution given logged events.
    '''
    def __init__(self, file_path, process_id):
        self.units = {}
        self.syncs = {}
        self.levels = {}
        self.sync_attempt_dates = []
        self.create_attempt_dates = []
        self.current_recv_sync_no = []
        self.process_id = process_id
        self.file_path = file_path
        self.memory_info = []
        self.start_date = None

    def set_start_date(self, date):
        if self.start_date is None:
            self.start_date = date
            self.levels[0] = {'date': date}

    def handle_event(self, event):
        ev_type = event['type']
        if ev_type == 'create_add':
            assert event['units'] not in self.units, "Unit hash collision?"
            self.set_start_date(event['date'])
            U = event['units']
            self.units[U] = {'created': event['date']}
            self.create_attempt_dates.append(event['date'])

        if ev_type == 'add_linear_order':
            level = event['level']
            self.levels[level]['n_units_decided'] = event['n_units']
            for U in event['units']:
                assert U in self.units, f"Unit {U} being added to linear order, but its appearance not noted."
                self.units[U]['ordered'] = event['date']

        if ev_type == 'add_foreign':
            U = event['units']
            if U not in self.units:
                self.units[U] = {'received': [event['date']]}
            else:
                U_dict = self.units[U]
                assert 'created' not in U_dict, f"Unit created by {self.process_id} later also received from another process."
                U_dict['received'].append(event['date'])

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
            self.syncs[sync_id] = {}
            self.syncs[sync_id]['start_date'] = event['date']

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
            self.sync_attempt_dates.append(event['date'])

        if ev_type == 'listener_sync_no':
            self.current_recv_sync_no.append(event['n_recv_syncs'])





    def analyze(self):
        log_parser = LogParser(self.file_path)
        for event in log_parser.get_events():
            if event['process_id'] != self.process_id:
                continue
            self.handle_event(event)


    def get_delays_create_order(self):
        delay_list = []
        for U, U_dict in self.units.items():
            if 'created' in U_dict and 'ordered' in U_dict:
                diff = diff_in_seconds(U_dict['created'], U_dict['ordered'])
                delay_list.append(diff)

        return delay_list

    def get_delays_add_foreign_order(self):
        delay_list = []
        for U, U_dict in self.units.items():
            if 'received' in U_dict and 'ordered' in U_dict:
                diff = diff_in_seconds(U_dict['received'][0], U_dict['ordered'])
                delay_list.append(diff)

        return delay_list

    def get_new_level_times(self):
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
        returns 4 lists:
        [level],
        [n_units decided at this level],
        [+levels of timing decision at this level],
        [time in sec to timing decision)
        '''
        levels = []
        n_units_per_level = []
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
        return levels, n_units_per_level, levels_plus_decided, level_delays

    def get_sync_info(self, plot_file = None):

        units_sent_per_sync = []
        units_received_per_sync = []
        time_per_sync = []
        syncs_not_succeeded = 0
        time_per_unit_exchanged = []
        bytes_per_unit_exchanged = []

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

        if plot_file is not None:
            fig, ax = plt.subplots()
            units_exchanged = [s + r for (s,r) in zip(units_sent_per_sync, units_received_per_sync)]
            x_series, y_series = units_exchanged, time_per_sync
            ax.scatter(x_series, y_series)
            ax.set(xlabel='#units', ylabel='sync time (sec)', title='Units exchanged vs sync time')
            fig.savefig(plot_file)

        return units_sent_per_sync, units_received_per_sync, time_per_sync, time_per_unit_exchanged, bytes_per_unit_exchanged, syncs_not_succeeded


    def get_memory_usage_vs_poset_size(self, plot_file=None, show_plot=False):
        '''
        Returns a list of pairs: ( poset size (#units) , memory usage in MiB)
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

    def prepare_basic_report(self, report_file = 'report.txt'):
        self.analyze()

        lines = []
        fields = ["name", "avg", "min", "max", "stdev", "n_samples"]
        lines.append(format_line(fields))

        def _append_stat_line(data, name):
            nonlocal fields, lines
            stats = compute_basic_stats(data)
            stats['name'] = name
            lines.append(format_line(fields, stats))



        # timing_decision
        levels, n_units_per_level, levels_plus_decided, level_delays = self.get_timing_decision_stats()
        _append_stat_line(n_units_per_level, 'n_units_decision')
        _append_stat_line(level_delays, 'time_decision')
        _append_stat_line(levels_plus_decided, 'decision_height')

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
        sent_per_sync, recv_per_sync, time_per_sync, time_per_unit_ex, bytes_per_unit_ex, syncs_not_succ = self.get_sync_info('sync_data.png')
        _append_stat_line(sent_per_sync, 'units_sent_sync')
        _append_stat_line(recv_per_sync, 'units_recv_sync')
        _append_stat_line(time_per_sync, 'time_per_sync')
        _append_stat_line(time_per_unit_ex, 'time_per_unit_ex')
        _append_stat_line(bytes_per_unit_ex, 'bytes_per_unit_ex')
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





        with open(report_file, "w") as report_file:
            for line in lines:
                report_file.write(line+'\n')



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
    summ['stdev'] = np.std(np_array)
    summ['min'] = np.min(np_array)
    summ['max'] = np.max(np_array)

    return summ

def format_line(field_list, data = None):
    if data == []:
        # to avoid problems with empty data
        data = [-1]
    line = ''
    for field in field_list:
        if data is None:
            value = field
        else:
            value = data[field]

        if isinstance(value, str):
            entry = value
        else:
            entry = f"{float(value):.4f}"
        just_len = 25 if field == 'name' else 12
        entry = entry.ljust(just_len)
        line += entry
    return line


