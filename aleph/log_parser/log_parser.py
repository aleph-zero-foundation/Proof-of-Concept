from parse import parse, compile
from datetime import datetime

import matplotlib.pyplot as plt



def diff_in_seconds(date_from, date_to):
    return (date_to-date_from).total_seconds()

class LogAnalyzer:
    '''
    A class for producing statistics about the protocol execution given logged events.
    '''
    def __init__(self, file_path, process_id):
        self.units = {}
        self.syncs = {}
        self.levels = {}
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

        if ev_type == 'add_linear_order':
            level = event['level']
            self.levels[level]['n_units_decided'] = event['n_units']
            for U in event['units']:
                assert U in self.units, f"Unit {U} being added to linear order, but its appearance not noted."
                self.units[U]['ordered'] = event['date']

        if ev_type == 'add_foreign':
            U = event['units']
            #print(U)
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


    def analyze(self):
        log_parser = LogParser(self.file_path)
        for event in log_parser.get_events():
            if event['process_id'] != self.process_id:
                continue
            self.handle_event(event)
            #print(event)


    def get_delays_create_order(self):
        delay_list = []
        for U, U_dict in self.units.items():
            #print(U_dict)
            if 'created' in U_dict and 'ordered' in U_dict:
                diff = diff_in_seconds(U_dict['created'], U_dict['ordered'])
                delay_list.append(diff)

        return delay_list

    def get_delays_add_foreign_order(self):
        delay_list = []
        for U, U_dict in self.units.items():
            #print(U_dict)
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
        returns the list of tuples:
        (level, n_units decided at this level, +levels of timing decision, time in sec to timing decision)
        '''
        data = []
        for level in self.levels:
            if level == 0:
                # timing is not decided on level
                continue
            else:
                if 'timing_decided_date' in self.levels[level]:
                    n_units = self.levels[level]['n_units_decided']
                    delay = diff_in_seconds(self.levels[level]['date'], self.levels[level]['timing_decided_date'])
                    level_diff = self.levels[level]['timing_decided_level'] - level
                    data.append((level, n_units, level_diff, delay))

        return data


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
        return data

    def prepare_basic_report():
        pass









def get_tokens(space_separated_string):
    return [s.strip() for s in space_separated_string.split()]

def parse_unit_list(space_separated_units):
    return [s[1:-1] for s in space_separated_units.split()]



class LogParser:

    def __init__(self, file_path):
        self.msg_pattern =  compile("[{date}] [{msg_level}] [{name}] {msg} [{file_line}]")
        self.split_on_bar = compile("{left}|{right}")
        self.file_path = file_path

    def parse_create(self, ev_type, ev_params, msg_body, event):
        pattern_create = compile("Created a new unit <{unit}>")
        event['units'] = pattern_create.search(msg_body)['unit']

    def parse_mem_usg(self, ev_type, ev_params, msg_body, event):
        pattern_memory = compile("{usage:d} MiB")
        event['memory'] = pattern_memory.search(msg_body)['usage']

    def parse_new_level(self, ev_type, ev_params, msg_body, event):
        pattern_level = compile("Level {level:d} reached")
        event['level'] = pattern_level.parse(msg_body)['level']

    def parse_add_foreign(self, ev_type, ev_params, msg_body, event):
        pattern_add_foreign = compile("trying to add <{unit}> from {ex_id} to poset")
        event['sync_id'] = int(ev_params[1])
        event['units'] = pattern_add_foreign.parse(msg_body)['unit']

    def parse_add_lin_order(self, ev_type, ev_params, msg_body, event):
        pattern_add_line = compile("At lvl {timing_level:d} added {n_units:d} units to the linear order {unit_list}")
        parsed = pattern_add_line.parse(msg_body)
        event['n_units'] = parsed['n_units']
        event['units'] = parse_unit_list(parsed['unit_list'])
        event['level'] = parsed['timing_level']
        assert event['n_units'] == len(event['units'])

    def parse_decide_timing(self, ev_type, ev_params, msg_body, event):
        pattern_decide_timing = compile("Timing unit for lvl {level:d} decided at lvl + {plus_level:d}")
        parsed = pattern_decide_timing.parse(msg_body)
        event['level'] = parsed['level']
        event['timing_decided_level'] = parsed['level'] + parsed['plus_level']


    def event_from_log_line(self, line):
        parsed_line =  self.msg_pattern.parse(line)
        assert parsed_line is not None
        event = parsed_line.named

        msg = event['msg']
        split_msg = self.split_on_bar.parse(msg)
        if split_msg is None:
            # this happens when some log message is not formatted using "|"
            # it means we should skip it
            return None
        event_descr, msg_body = split_msg['left'].strip(), split_msg['right'].strip()
        #print(event_descr)

        ev_tokens = get_tokens(event_descr)

        ev_type = ev_tokens[0]

        parse_mapping = {
            'create_add' : self.parse_create,
            'memory_usage' : self.parse_mem_usg,
            'add_linear_order' : self.parse_add_lin_order,
            'new_level' : self.parse_new_level,
            'add_foreign' : self.parse_add_foreign,
            'decide_timing' : self.parse_decide_timing,
            }
        if ev_type in parse_mapping:
            event['type'] = ev_type
            if len(ev_tokens) > 1:
                event['process_id'] = int(ev_tokens[1])
            parse_mapping[ev_type](ev_type, ev_tokens[1:], msg_body, event)
        else:
            return None
        event.pop('msg', None)
        event.pop('msg_level', None)
        event.pop('name', None)
        event['date'] = datetime.strptime(event['date'], "%Y-%m-%d %H:%M:%S,%f")
        return event



    def get_events(self):
        with open(self.file_path, "r") as log_file:
            for line in log_file:
                e = self.event_from_log_line(line.strip())
                if e is not None:
                    yield e
                    #evs.append(e)
        #for e in evs:
        #    print(e)
        #print(cnt)




path = r'../../tests/aleph.log'
analyzer = LogAnalyzer(path, process_id = 0)
analyzer.analyze()
print(analyzer.get_delays_create_order())
print(analyzer.get_delays_add_foreign_order())
print(analyzer.get_new_level_times())
print(analyzer.get_memory_usage_vs_poset_size('memory.png'))
print(analyzer.get_timing_decision_stats())