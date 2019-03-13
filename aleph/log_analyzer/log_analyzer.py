from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np
import os
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
    def __init__(self, file_path, process_id = None, generate_plots = True):
        self.units = {}
        self.syncs = {}
        self.levels = {}
        self.sync_attempt_dates = []
        self.create_attempt_dates = []

        self.create_times = []
        self.max_units_cnts = []
        self.n_create_fail = 0
        self.timing_attempt_times = []

        self.prime_learned_times_per_level = {}

        self.current_recv_sync_no = []
        self.read_process_id = process_id
        self.file_path = file_path
        self.memory_info = []
        self.start_date = None
        self.add_run_times = []
        self.process_id = None

        self.generate_plots = generate_plots

        self.prepare_parsers()

        # create the mapping between event types and the functions used for parsing this types of events
        self.parse_mapping = {
            'create_add' : self.parse_create,
            'memory_usage' : self.parse_mem_usg,
            'add_linear_order' : self.parse_add_lin_order,
            'new_level' : self.parse_new_level,
            'decide_timing' : self.parse_decide_timing,
            'sync_establish' : self.parse_establish,
            'listener_establish' : self.parse_establish,
            'sync_succ' : self.parse_listener_succ,
            'listener_succ' : self.parse_listener_succ,

            'receive_units_done_sync' : self.receive_units_done_parser,
            'receive_units_done_listener' : self.receive_units_done_parser,

            'sync_establish_try' : self.parse_try_sync,
            'listener_sync_no' : self.parse_listener_sync_no,
            'add_run_time' : self.parse_add_run_time,
            'start_process' : self.parse_start_process,
            'receive_units_start_listener': self.parse_receive_units_start,
            'receive_untis_start_sync' : self.parse_receive_units_start,
            'add_received_done_listener' : self.parse_add_received_done,
            'add_received_done_sync' : self.parse_add_received_done,
            'timer' : self.parse_timer,
            'max_units' : self.parse_max_units,
            'create_fail' : self.parse_create_fail,
            'prime_unit' : self.parse_prime_unit,

            'send_poset_sync' : self.parse_network_operation('send_poset_info', is_start=True),
            'send_poset_wait_sync' : self.send_poset_wait_parser,
            'send_poset_done_sync' : self.send_poset_done_parser,

            'send_poset_listener' : self.parse_network_operation('send_poset_info', is_start=True),
            'send_poset_wait_listener' : self.send_poset_wait_parser,
            'send_poset_done_listener' : self.send_poset_done_parser,

            'send_requests_start_sync' : self.parse_network_operation('send_requests', is_start=True),
            'send_requests_wait_sync' : self.send_requests_wait_parser,
            'send_requests_done_sync' : self.send_requests_done_parser,

            'send_requests_start_listener' : self.parse_network_operation('send_requests', is_start=True),
            'send_requests_wait_listener' : self.send_requests_wait_parser,
            'send_requests_done_listener' : self.send_requests_done_parser,

            'send_units_start_sync' : self.parse_network_operation('send_units', is_start=True),
            'send_units_wait_sync' : self.send_units_wait_parser,
            'send_units_sent_sync' : self.send_units_sent_parser,
            'send_units_done_sync' : self.parse_network_operation('send_units'),

            'send_units_start_listener' : self.parse_network_operation('send_units', is_start=True),
            'send_units_wait_listener' : self.send_units_wait_parser,
            'send_units_sent_listener' : self.send_units_sent_parser,
            'send_units_done_listener' : self.parse_network_operation('send_units'),


            'receive_requests_done_sync' :
            self.parse_bytes_processed_in_sync('receive_requests',
                                               self.parse_bytes_received_requests,
                                               is_start=True),
            'receive_requests_done_listener' :
            self.parse_bytes_processed_in_sync('receive_requests',
                                               self.parse_bytes_received_requests,
                                               is_start=True),
            'receive_poset_sync' : self.receive_poset_info_parser,
            'receive_poset_listener' : self.receive_poset_info_parser
            }

    def create_msg_pattern(self):
        # NOTE: 'parse' always matches the shortest text necessary (from left to right) to fulfill the parse pattern due to this
        #       behavior some logs are not parsed correctly, e.g. ones including pretty printed lists (file pattern is not
        #       parsed correctly). The following code is a 'dirty' fix for this behavior.

        # try to parse a file name included in every log line, i.e. a pattern of the form [filename.py:linenumber] (e.g.
        # process.py:1)
        @parse.with_pattern(r'[a-zA-Z0-9_]+\.py:\d+')
        def file_line_parser(text):
            return text
        return parse.compile("[{date}] [{msg_level}] [{name}] {msg} [{file_line:file_line_type}]",
                             dict(file_line_type=file_line_parser))

    def prepare_parsers(self):
        # initialize a bunch of parsing patterns
        # doing parse.compile() beforehand once is to speed-up the parsing
        self.msg_pattern = self.create_msg_pattern()
        self.split_on_bar = parse.compile("{left}|{right}")

        self.pattern_create = parse.compile("Created a new unit <{unit}> with {n_parents:d} parents")
        self.pattern_memory = parse.compile("{usage:f} MiB")
        self.pattern_level = parse.compile("Level {level:d} reached")
        self.pattern_add_line = parse.compile("At lvl {timing_level:d} added {n_units:d} units and {n_txs:d} txs to the linear "
                                              "order {unit_list}")
        self.pattern_decide_timing = parse.compile("Timing unit for lvl {level:d} decided at lvl + {plus_level:d}, poset lvl + "
                                                   "{plus_poset_level:d}")
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
        self.pattern_send_poset_info_bytes = parse.compile("sent heights {to_send} ({n_bytes:d} bytes) to {process_id:d}")
        self.pattern_send_requests_bytes = parse.compile("sent requests {to_send} ({n_bytes:d} bytes) to {process_id:d}")
        self.pattern_receive_requests_bytes = parse.compile("received requests {requests_received} ({n_bytes:d} bytes) from "
                                                            "{process_id:d}")
        self.pattern_receive_poset_info_bytes = parse.compile("Got heights {info} ({n_bytes:d} bytes) from {process_id:d}")
        self.pattern_max_units = parse.compile("There are {n_maximal:d} maximal units just before create_unit")
        self.pattern_create_fail = parse.compile("Failed to create a new unit")
        self.pattern_prime_unit = parse.compile("New prime unit at level {level:d} : {unit}")

        # helper functions related with parsing
        def parse_value_using_parser(parser, tag):
            def parser_method(msg_body):
                parsed = parser.parse(msg_body)
                if parsed is None:
                    return (False, None)
                return True, parsed[tag]
            return parser_method

        self.parse_bytes_send_units = parse_value_using_parser(self.pattern_send_units_sent, 'n_bytes')

        self.parse_bytes_send_poset_info = parse_value_using_parser(self.pattern_send_poset_info_bytes, 'n_bytes')

        self.parse_bytes_send_requests = parse_value_using_parser(self.pattern_send_requests_bytes, 'n_bytes')

        self.parse_bytes_received_units = parse_value_using_parser(self.pattern_receive_units_done, 'n_bytes')

        self.parse_bytes_received_requests = parse_value_using_parser(self.pattern_receive_requests_bytes, 'n_bytes')

        self.parse_bytes_received_poset_info = parse_value_using_parser(self.pattern_receive_poset_info_bytes, 'n_bytes')

        def combine_parsers(*parsers):
            def parse(ev_params, msg_body, event):
                for parser in parsers:
                    parser(ev_params, msg_body, event)
            return parse

        # prepare some common parsers used in the following parts of the initialization
        self.receive_units_done_parser = combine_parsers(
            self.parse_receive_units_done,
            self.parse_bytes_processed_in_sync('receive_units', self.parse_bytes_received_units)
        )

        self.send_poset_done_parser = combine_parsers(
            self.parse_network_operation('send_poset_info'),
            self.parse_await_time('send_poset_info'),
            self.parse_bytes_processed_in_sync('send_poset_info', self.parse_bytes_send_poset_info),
        )

        self.send_poset_wait_parser = combine_parsers(
            self.parse_await_time('send_poset_info', is_start=True),
            self.parse_bytes_processed_in_sync('send_poset_info', lambda _: (True, 0), is_start=True)
        )

        self.send_requests_wait_parser = combine_parsers(
            self.parse_bytes_processed_in_sync('send_requests', lambda _: (True, 0), is_start=True),
            self.parse_await_time('send_requests', is_start=True)
        )

        self.send_requests_done_parser = combine_parsers(
            self.parse_bytes_processed_in_sync('send_requests', self.parse_bytes_send_requests),
            self.parse_await_time('send_requests'),
            self.parse_network_operation('send_requests'),
        )

        self.send_units_wait_parser = combine_parsers(
            self.parse_await_time('send_units', is_start=True),
            self.parse_bytes_processed_in_sync('send_units', lambda _: (True, 0), is_start=True)
        )

        self.send_units_sent_parser = combine_parsers(
            self.parse_bytes_processed_in_sync('send_units', self.parse_bytes_send_units),
            self.parse_await_time('send_units'),
            self.parse_send_units_sent
        )

        def parse_if_includes_message(msg_parser, guarded_parser):
            def parser(ev_params, msg_body, event):
                if not msg_parser(msg_body)[0]:
                    return
                guarded_parser(ev_params, msg_body, event)
            return parser

        self.receive_poset_info_parser = combine_parsers(
            parse_if_includes_message(self.parse_bytes_received_poset_info, self.parse_network_operation('receive_poset_info', is_start=True)),
            self.parse_bytes_processed_in_sync('receive_poset_info',
                                               self.parse_bytes_received_poset_info,
                                               is_start=True),
            parse_if_includes_message(self.parse_bytes_received_poset_info, self.parse_network_operation('receive_poset_info'))
        )

    # Functions for parsing specific types of log messages. Typically one function per log message type.
    # Although some functions support more of them at the same time
    # Each of these functions takes the same set of parameters, being:
    # -- ev_params: parameters of the log event, typically process_id or/and sync_id
    # -- msg_body: the remaining part of the log message that carries information specific to this event type
    # -- event: this is the partial result of parsing of the current line, it has a date field etc.

    # ----------- START PARSING FUNCTIONS ---------------

    def parse_await_time(self, event_name, is_start=False):
        def parse(ev_params, msg_body, event):
            def process_report(report):
                if is_start:
                    report['await'] = self.create_await(event['date'])
                else:
                    assert 'await' in report, "Missing starting event for 'await'"
                    report['await']['stop_date'] = event['date']

            self.process_network_report(event_name, process_report)(ev_params, msg_body, event)

        return parse

    def parse_bytes_processed_in_sync(self, event_name, retrieve_bytes=lambda msg: (False, 0), is_start=False):
        def parse(ev_params, msg_body, event):
            (parsed, n_bytes) = retrieve_bytes(msg_body)
            if not parsed:
                return

            def process_report(report):
                report['n_bytes'] = n_bytes
                if is_start:
                    report['start_date'] = event['date']
                else:
                    report['stop_date'] = event['date']

            self.process_network_report(event_name, process_report)(ev_params, msg_body, event)

        return parse

    def parse_network_operation(self, event_name, is_start=False):
        def parse(ev_params, msg_body, event):
            sync_id = int(ev_params[1])
            sync_events = self.get_or_create_events(sync_id, event_name, event['date'])
            if is_start:
                new_event = self.create_event(event_name)
                new_event['start_date'] = event['date']
                sync_events.append(new_event)
            else:
                assert sync_events, f"There was no starting event for {event_name}"
                assert sync_events[-1]['event_name'] == event_name, f'Wrong name of the last event'
                sync_events[-1]['stop_date'] = event['date']

        return parse

    def parse_max_units(self, ev_params, msg_body, event):
        parsed = self.pattern_max_units.parse(msg_body)
        self.max_units_cnts.append(parsed['n_maximal'])

    def parse_create_fail(self, ev_params, msg_body, event):
        parsed = self.pattern_create_fail.parse(msg_body)
        assert parsed, "Failed to parse a message of an 'create_fail' event"
        self.n_create_fail += 1

    def parse_prime_unit(self, ev_params, msg_body, event):
        parsed = self.pattern_prime_unit.parse(msg_body)
        level = parsed['level']
        if level not in self.prime_learned_times_per_level:
            self.prime_learned_times_per_level[level] = []
        date = event['date']
        self.prime_learned_times_per_level[level].append(date)

    def parse_create(self,  ev_params, msg_body, event):
        parsed = self.pattern_create.parse(msg_body)
        U = parsed['unit']
        assert U not in self.units, "Unit hash collision?"
        if self.levels == {}:
            self.levels[0] = {'date' : event['date']}
        self.units[U] = {'created': event['date'], 'n_parents': parsed['n_parents']}
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
            elif timer_name == "attempt_timing":
                self.timing_attempt_times.append(time_spent)
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
                if 'created' in U_dict:
                    continue
                #assert 'created' not in U_dict, f"Unit created by {self.read_process_id} later also received from another process."
                if 'received' not in U_dict:
                    U_dict['received'] = [event['date']]
                else:
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
        self.syncs[sync_id] = self.create_sync_event(start_date=event['date'], target=target, tried=True)

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
            if U not in self.units:
                # this can happen and should not trigger an exception because timing units are logged before the list of units received in one sync
                #assert U in self.units, f"Unit {U} being added to linear order, but its appearance not noted."
                self.units[U] = {}
            self.units[U]['ordered'] = event['date']

    def parse_decide_timing(self, ev_params, msg_body, event):
        parsed = self.pattern_decide_timing.parse(msg_body)
        level = parsed['level']
        timing_decided_level = parsed['level'] + parsed['plus_level']
        timing_poset_decided_level = parsed['level'] + parsed['plus_poset_level']

        self.levels[level]['timing_decided_level'] = timing_decided_level
        self.levels[level]['timing_poset_decided_level'] = timing_poset_decided_level
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

    def create_sync_event(self, start_date, target=None, events=None, tried=None):
        if events is None:
            events = []
        result = {'start_date': start_date, 'events': events}
        if target:
            result['target'] = target
        if tried:
            result['tried'] = tried
        return result

    def create_network_report(self, n_bytes, start_date):
        return dict(n_bytes=n_bytes, start_date=start_date)

    def get_or_create_events(self, sync_id, event_name, start_date):
        sync = self.syncs.get(sync_id, None)
        if sync is None:
            sync = self.create_sync_event(start_date)
            self.syncs[sync_id] = sync
        return sync['events']

    def create_event(self, event_name):
        return dict(event_name=event_name)

    def create_await(self, start_date):
        return dict(start_date=start_date)

    def process_network_report(self, event_name, report_handler=lambda _: None):
        def parse(ev_params, msg_body, event):
            sync_id = int(ev_params[1])
            events = self.get_or_create_events(sync_id, event_name, event['date'])
            if (not events) or (events[-1]['event_name'] != event_name):
                events.append(self.create_event(event_name))
            last_event = events[-1]
            report = last_event.get('network_report', None)
            if report is None:
                report = self.create_network_report(0, start_date=event['date'])
                last_event['network_report'] = report
            report_handler(report)

        return parse

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

            if self.read_process_id is not None and event['process_id'] != self.read_process_id:
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

    def get_txps_till_last_timing_unit(self):
        '''
        Returns the number of transactions per second averaged from start till deciding on the timing unit at lvl 1.
        '''
        levels_with_timing = [level for level in self.levels if 'timing_decided_date' in self.levels[level]]
        if levels_with_timing == []:
            return 0.0

        max_level = max(levels_with_timing)
        # we use range(1, max_level+1) since nothing is decided on lvl 0
        tot_txs = sum(self.levels[level]['n_txs_ordered'] for level in range(1, max_level+1))
        secs_till_max_level_timing = diff_in_seconds(self.levels[0]['date'], self.levels[max_level]['timing_decided_date'])
        return tot_txs/secs_till_max_level_timing

    def build_bytes_per_second_stats(self, events, window_in_seconds=1):
        events = sorted(events, key=lambda x: x['start_date'])
        series = [0]
        last_timestamp = events[0]['start_date']
        for event in events:
            while not (diff_in_seconds(last_timestamp, event['start_date']) <= window_in_seconds):
                series.append(0)
                last_timestamp += timedelta(0, window_in_seconds)
            series[-1] += event['n_bytes']

        return series

    def get_outbound_network_events(self):
        return [event['network_report']
                for _, sync in self.syncs.items()
                for event in sync.get('events', [])
                if 'network_report' in event and event['event_name'].startswith('send')]

    def get_inbound_network_events(self):
        return [event['network_report']
                for _, sync in self.syncs.items()
                for event in sync.get('events', [])
                if 'network_report' in event and event['event_name'].startswith('receive')]

    def plot_network_utilization(self, network_plot_outbound_file=None, network_plot_inbound_file=None):
        if not self.generate_plots:
            return

        def plot_network(events, y_label='bytes sent', network_plot_file=network_plot_outbound_file):
            if not events:
                return

            series = self.build_bytes_per_second_stats(events, window_in_seconds=1)

            x_series = list(range(len(series)))
            y_series = series
            plt.bar(x_series, y_series)
            plt.xlabel('time in seconds')
            plt.ylabel(y_label)
            plt.savefig(network_plot_file, dpi=800)
            plt.close()
            print(f'Plot written to {network_plot_file}.')

        # plotting outbound traffic
        if network_plot_outbound_file is not None:
            events = self.get_outbound_network_events()
            plot_network(events, network_plot_file=network_plot_outbound_file)

        # plotting inbound traffic
        if network_plot_inbound_file is not None:
            events = self.get_inbound_network_events()
            plot_network(events, 'bytes received', network_plot_file=network_plot_inbound_file)

    def get_cpu_times(self,
                      cpu_plot_file = None,
                      cpu_io_plot_file = None,
                      cpu_network_rest_plot_file = None):
        timer_names = ['t_prepare_units', 't_compress_units', 't_decompress_units', 't_unpickle_units',
                     't_pickle_units', 't_verify_signatures', 't_add_units']
        cpu_time_summary = { name : [] for name in timer_names }
        cpu_time_summary['t_tot_sync'] = []
        cpu_time_summary['t_order_level'] = []

        total_add_unit = 0.0
        n_add_unit = 0

        cpu_breakdown_entries = []
        cpu_io_breakdown = []
        # decomposition of time spent in sync between cpu/(sending operations)/rest
        cpu_network_rest_breakdown = []

        for sync_id, sync_dict in sorted(self.syncs.items(), key = lambda x: x[0]):
            sync_tot_cpu_time = 0.0
            sync_tot_network_time = 0.0
            entry = []
            for name in timer_names:
                if name in sync_dict:
                    cpu_time_summary[name].append(sync_dict[name])
                    sync_tot_cpu_time += sync_dict[name]
                entry.append(sync_dict.get(name, 0.0))
            for event in sync_dict.get('events', []):
                network_report = event.get('network_report', None)
                if network_report is None:
                    continue
                await_event = network_report.get('await', None)
                if not await_event or 'stop_date' not in await_event:
                    continue
                sync_tot_network_time += diff_in_seconds(await_event['start_date'], await_event['stop_date'])
            if sync_tot_cpu_time > 0.0:
                cpu_time_summary['t_tot_sync'].append(sync_tot_cpu_time)
                cpu_breakdown_entries.append(entry)
            if sync_tot_cpu_time > 0.0 and 'stop_date' in sync_dict:
                sync_tot_time = diff_in_seconds(sync_dict['start_date'], sync_dict['stop_date'])
                cpu_io_breakdown.append([sync_tot_cpu_time, sync_tot_time - sync_tot_cpu_time])
                cpu_network_rest_breakdown.append([sync_tot_cpu_time,
                                                 sync_tot_network_time,
                                                 sync_tot_time - sync_tot_cpu_time - sync_tot_network_time])

        for level, level_dict in self.levels.items():
            if 't_lin_order' in level_dict:
                cpu_time_summary['t_order_level'].append(level_dict['t_lin_order'])

        cpu_time_summary['t_create'] = self.create_times
        cpu_time_summary['t_attempt_timing'] = self.timing_attempt_times

        def plot_io_breakdown(plot_file, *data):
            if not data:
                return
            layers = []
            n_syncs = len(data[0][1])
            x_series = range(n_syncs)
            heights = [0.0] * n_syncs
            width = 0.5
            for plot_data in data:
                plot = plt.bar(x_series, plot_data[1], width, label=plot_data[0], bottom=heights)
                layers.append(plot)
                heights = [plot_data[1][i] + heights[i] for i in range(n_syncs)]
            plt.legend(handles=layers)
            plt.savefig(plot_file, dpi=800)
            plt.close()

        # the plot showing how the cpu time is divided between various tasks
        if self.generate_plots and cpu_breakdown_entries != []:
            plot_io_breakdown(cpu_plot_file, *[(
                                                timer_names[ind],
                                                [cpu_breakdown_entries[i][ind] for i in range(len(cpu_breakdown_entries))]
                                               )
                                               for ind in range(len(timer_names))])

        # the plot showing how the sync time divides into cpu vs non-cpu
        if self.generate_plots and cpu_io_breakdown != []:
            n_syncs = len(cpu_io_breakdown)
            y_series_cpu = [cpu_io_breakdown[i][0] for i in range(n_syncs)]
            y_series_rest = [cpu_io_breakdown[i][1] for i in range(n_syncs)]
            plot_io_breakdown(cpu_io_plot_file, ('cpu_time', y_series_cpu), ('io+rest', y_series_rest))

        # the plot showing how the sync time divides into cpu vs (network operations vs rest)
        if self.generate_plots and cpu_network_rest_breakdown:
            n_syncs = len(cpu_io_breakdown)
            cpu_series = [cpu_network_rest_breakdown[i][0] for i in range(n_syncs)]
            network_series = [cpu_network_rest_breakdown[i][1] for i in range(n_syncs)]
            rest_series = [cpu_network_rest_breakdown[i][2] for i in range(n_syncs)]
            plot_io_breakdown(cpu_network_rest_plot_file,
                              ('cpu_time', cpu_series),
                              ('network_send', network_series),
                              ('rest', rest_series))

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

    def get_delays_learn_prime_quorum(self):
        '''
        Computes delays between learning about the first prime unit at a given level and learning the prime unit no (2/3)*N.
        '''
        delay_list = []
        for level in self.prime_learned_times_per_level:
            dates = self.prime_learned_times_per_level[level]
            threshold = (2*self.n_processes+2)//3
            if len(dates) >= threshold:
                # Note that the timestamps are added to the list in chronological order, so the list is sorted
                delay_list.append(diff_in_seconds(dates[0], dates[threshold-1]))

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
        [+levels of poset at the time of decision],
        [time in sec to timing decision)
        [number of transactions at this level]
        '''
        levels = []
        n_units_per_level = []
        n_txs_per_level = []
        levels_plus_decided = []
        levels_poset_plus_decided = []
        level_delays = []
        for level in self.levels:
            if level == 0:
                # timing is not decided on level
                continue
            else:
                if 'n_units_decided' in self.levels[level]:
                    n_units = self.levels[level]['n_units_decided']
                    delay = diff_in_seconds(self.levels[level]['date'], self.levels[level]['timing_decided_date'])
                    level_diff = self.levels[level]['timing_decided_level'] - level
                    poset_level_diff = self.levels[level]['timing_poset_decided_level'] - level
                    levels.append(level)
                    n_units_per_level.append(n_units)
                    levels_plus_decided.append(level_diff)
                    levels_poset_plus_decided.append(poset_level_diff)
                    level_delays.append(delay)
                    n_txs_per_level.append(self.levels[level]['n_txs_ordered'])
        print(n_units_per_level)
        return levels, n_units_per_level, levels_plus_decided, levels_poset_plus_decided, level_delays, n_txs_per_level

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
        -- send_poset_info_not_succeeded: one int - the number of send_poset_info rounds that were started but for some reason
           did not terminate successfully
        -- send_units_not_succeeded: one int - the number of send_units rounds that were started but for some reason did not
           terminate successfully
        -- send_requests_not_succeeded: one int - the number of send_requests rounds that were started but for some reason did
           not terminate successfully
        -- bytes_sent_per_sync: the (list of) number of bytes sent by sync
        '''
        units_sent_per_sync = []
        units_received_per_sync = []
        time_per_sync = []
        syncs_not_succeeded = 0
        send_poset_info_not_succeeded = 0
        send_requests_not_succeeded = 0
        send_units_not_succeeded = 0
        bytes_sent_per_sync = []
        time_per_unit_exchanged = []
        bytes_per_unit_exchanged = []
        establish_connection_times = []

        for sync_id, sync in sorted(self.syncs.items(), key = lambda x: x[0]):
            bytes_sent_per_sync.append(self.retrieve_bytes_sent(sync['events']))
            if 'stop_date' not in sync:
                syncs_not_succeeded += 1

                for event in (e for e in sync['events'] if self.is_event_failed(e)):
                    name = event['event_name']
                    if name == 'send_poset_info':
                        send_poset_info_not_succeeded += 1
                    if name == 'send_requests':
                        send_requests_not_succeeded += 1
                    if name == 'send_units':
                        send_units_not_succeeded += 1

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


        if self.generate_plots:
            fig, ax = plt.subplots()
            units_exchanged = [s + r for (s,r) in zip(units_sent_per_sync, units_received_per_sync)]
            x_series, y_series = units_exchanged, time_per_sync
            ax.scatter(x_series, y_series, s=1)
            ax.set(xlabel='#units', ylabel='sync time (sec)', title='Units exchanged vs sync time')
            fig.savefig(plot_file, dpi=500)
            plt.close()

        return units_sent_per_sync, units_received_per_sync, time_per_sync, \
            time_per_unit_exchanged, bytes_per_unit_exchanged, \
            establish_connection_times, syncs_not_succeeded, \
            send_poset_info_not_succeeded, send_units_not_succeeded, \
            send_requests_not_succeeded, bytes_sent_per_sync



    def get_memory_usage_vs_poset_size(self, plot_file=None):
        '''
        Returns a list of memory usages (in MiB) of the python process at regular times.
        '''

        data = []
        for entry in self.memory_info:
            data.append((entry['poset_size'], entry['memory']))

        if self.generate_plots:
            fig, ax = plt.subplots()
            x_series, y_series = [point[0] for point in data], [point[1] for point in data]
            ax.plot(x_series, y_series)
            ax.set(xlabel='#units', ylabel='usage (MiB)', title='Memory Consumption')
            if plot_file is not None:
                fig.savefig(plot_file)
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

    def get_n_parents(self):
        '''
        Returns statistics regarding the number of parents of units created by the tracked process.
        '''
        n_parents_list = []
        for U, U_dict in self.units.items():
            if 'n_parents' in U_dict:
                n_parents_list.append(U_dict['n_parents'])

        return n_parents_list

    def gen_units_exchanged_plots(self, plot_file):
        if self.generate_plots:

            n_sent_list = []
            n_recv_list = []
            dates = []

            for sync_id, sync_dict in sorted(self.syncs.items(), key = lambda x: x[0]):
                n_sent = sync_dict.get('units_sent', 0)
                n_recv = sync_dict.get('units_received', 0)
                if n_sent > 0 or n_recv > 0:
                    n_sent_list.append(n_sent)
                    n_recv_list.append(n_recv)
                    dates.append(sync_dict['start_date'])
            ticks = 5
            jump_len = len(n_sent_list) // ticks
            x_series = range(len(n_sent_list))
            x_ticks = x_series[::jump_len]
            time_ticks = [diff_in_seconds(dates[0], dates[ind]) for ind in x_ticks]
            fig, (ax_sent, ax_recv) = plt.subplots(2,1)
            width = 1.0
            sent_bars = ax_sent.bar(x_series, n_sent_list, width, color = 'red')
            recv_bars = ax_recv.bar(x_series, n_recv_list, width, color = 'blue')

            plt.setp((ax_sent, ax_recv), xticks=x_ticks, xticklabels=time_ticks)
            #ax_recv.xticks(x_ticks, time_ticks)

            ax_sent.legend([sent_bars[0]], ['units sent'])
            ax_recv.legend([recv_bars[0]], ['units received'])

            ax_sent.set(xlabel='time from start (s)')
            ax_recv.set(xlabel='time from start (s)')
            plt.tight_layout()

            plt.savefig(plot_file, dpi=800)
            plt.close()

    def is_event_failed(self, event):
        return 'stop_date' not in event

    def get_successes_failures_count(self, event):
        succeded, failed = 0, 0
        for e in event:
            if not self.is_event_failed(e):
                succeded += 1
            else:
                failed += 1
        return succeded, failed

    def get_event_time(self, event):
        result = 0.0
        for e in event:
            if not self.is_event_failed(e):
                result += diff_in_seconds(e['start_date'], e['stop_date'])
        return result

    def retrieve_bytes_sent(self, events):
        bytes_sent = 0
        for event in events:
            if 'network_report' in event:
                bytes_sent += event['network_report']['n_bytes']
        return bytes_sent

    def prepare_report_per_process(self, dest_dir = 'reports', file_name_prefix = 'report-sync-'):
        '''
        Create the file with a a summary od synchronization statistics to all the remaining processes.
        :param string dest_dir: the path to the directory where the report file should be written
        :param string file_name_prefix: the prefix of the filename where the report should be written,
                                        process_id and the extension '.txt' is appended at the end
        WARNING: before calling this function call LogAnalyzer.analyze() first

        Meaning of the specific columns:

        - sync_fail (poset_info, units, requests): the total number of failed syncs + decomposition on individual rounds of sync
          (incoming and outcoming included)

        - sync_succ (poset_info, units, requests): the total number of succesful syncs + decomposition on individual rounds of
          sync (incoming and outcoming included)

        - avg_time (poset_info, units, requests): average time (per sync) spent on "talking" to a specific process +
          decomposition on individual rounds of sync

        - n_conn: the number of outcoming connection attempts to a specific process

        - sent_bytes: the total number of bytes sent
        '''

        syncs_failed = [0] * self.n_processes
        send_poset_info_failed = [0] * self.n_processes
        send_poset_info_succeeded = [0] * self.n_processes
        send_poset_info_avg = [0] * self.n_processes
        send_units_failed = [0] * self.n_processes
        send_units_succeeded = [0] * self.n_processes
        send_units_avg = [0] * self.n_processes
        send_requests_failed = [0] * self.n_processes
        send_requests_succeeded = [0] * self.n_processes
        send_requests_avg = [0] * self.n_processes
        syncs_succeeded = [0] * self.n_processes
        syncs_sent_data = [0] * self.n_processes
        tot_time = [0.0] * self.n_processes
        n_conn_est = [0] * self.n_processes
        for sync_id, sync in sorted(self.syncs.items(), key = lambda x: x[0]):
            if 'target' not in sync:
                # this sync has no info on which process are we talking to, cannot do much
                continue
            target = sync['target']

            if 'conn_est_time' in sync:
                n_conn_est[target] += 1

            events = sync.get('events', [])
            send_poset_info_events = [a for a in events if a['event_name'] == 'send_poset_info']
            poset_info_result = self.get_successes_failures_count(send_poset_info_events)
            send_poset_info_succeeded[target] += poset_info_result[0]
            send_poset_info_failed[target] += poset_info_result[1]

            send_units_events = [a for a in events if a['event_name'] == 'send_units']
            send_units_results = self.get_successes_failures_count(send_units_events)
            send_units_succeeded[target] += send_units_results[0]
            send_units_failed[target] += send_units_results[1]

            send_requests_events = [a for a in events if a['event_name'] == 'send_request ==\'send_requests']
            send_requests_result = self.get_successes_failures_count(send_requests_events)
            send_requests_succeeded[target] += send_requests_result[0]
            send_requests_failed[target] += send_requests_result[1]

            send_poset_info_avg[target] += self.get_event_time(send_poset_info_events)
            send_units_avg[target] += self.get_event_time(send_units_events)
            send_requests_avg[target] += self.get_event_time(send_requests_events)

            syncs_sent_data[target] += self.retrieve_bytes_sent(events)

            if 'stop_date' not in sync:
                syncs_failed[target] += 1

                continue

            syncs_succeeded[target] += 1
            tot_time[target] += diff_in_seconds(sync['start_date'], sync['stop_date'])

        fields = ['name',
                  'sync_fail (poset_info, units, requests)',
                  'sync_succ (poset_info, units, requests)',
                  'avg_time (poset_info, units, requests)',
                  'n_conn',
                  'sent_bytes']
        lines = []
        lines.append(format_line(fields))

        for target in range(self.n_processes):
            if syncs_succeeded[target]:
                tot_time[target] /= syncs_succeeded[target]
            else:
                tot_time[target] = -1.0
            if send_poset_info_succeeded[target]:
                send_poset_info_avg[target] /= send_poset_info_succeeded[target]
            else:
                send_poset_info_avg[target] = -1.0
            if send_units_succeeded[target]:
                send_units_avg[target] /= send_units_succeeded[target]
            else:
                send_units_avg[target] = -1.0
            if send_requests_succeeded[target]:
                send_requests_avg[target] /= send_requests_succeeded[target]
            else:
                send_requests_avg[target] = -1.0
            data = {'name' : f'proc_{target}'}

            data['sync_fail (poset_info, units, requests)'] = (
                f'{syncs_failed[target]} ('
                f'{send_poset_info_failed[target]}, '
                f'{send_units_failed[target]}, '
                f'{send_requests_failed[target]})'
            )
            data['sync_succ (poset_info, units, requests)'] = (
                f'{syncs_succeeded[target]} ('
                f'{send_poset_info_succeeded[target]}, '
                f'{send_units_succeeded[target]}, '
                f'{send_requests_succeeded[target]})'
            )

            def custom_float_format(main_value, *values):
                return f"{main_value:.4f}" + ' (' + ', '.join([f"{v:.4f}" for v in values]) + ')'

            data['avg_time (poset_info, units, requests)'] = custom_float_format(
                tot_time[target],
                send_poset_info_avg[target],
                send_units_avg[target],
                send_requests_avg[target]
            )
            data['n_conn'] = n_conn_est[target]
            data['sent_bytes'] = syncs_sent_data[target]

            lines.append(format_line(fields, data))

        report_file = os.path.join(dest_dir, 'txt-sync', file_name_prefix+str(self.process_id)+'.txt')
        os.makedirs(os.path.dirname(report_file), exist_ok=True)

        with open(report_file, "w") as rep_file:
            for line in lines:
                rep_file.write(line+'\n')

            print(f'Report file written to {report_file}.')

    def prepare_phases_report(self, reporter):
        phase_1_times = []
        phase_2_times = []
        phase_1_times_sync = []
        phase_1_times_listener = []
        phase_2_times_sync = []
        phase_2_times_listener = []
        sync_event_name = 'receive_poset_info'
        listener_event_name = 'send_poset_info'

        for sync_id, sync in self.syncs.items():
            events = sync.get('events', [])
            if not events:
                continue
            ending_event = ''
            start_date = sync['start_date']
            end_date = start_date
            event_name = events[0]['event_name']
            if event_name.startswith(sync_event_name):
                # search for matching 'send_poset_info' event
                ending_event = listener_event_name
            elif event_name.startswith(listener_event_name):
                # search for matching 'receive_poset_info' event
                ending_event = sync_event_name

            if ending_event == '':
                import sys
                print('Unknown version of the protocol (sync_id = {sync_id:d})', file=sys.stderr)
                continue

            enclosing_event = None
            for event in events:
                if event['event_name'].startswith(ending_event):
                    enclosing_event = event
                    break

            if not enclosing_event:
                import sys
                print(f'Missing enclosing event for the first phase of a sync {sync_id:d}', file=sys.stderr)
                continue

            end_date = enclosing_event['stop_date']
            phase_1_time = diff_in_seconds(start_date, end_date)

            phase_1_times.append(phase_1_time)

            if ending_event == sync_event_name:
                phase_1_times_sync.append(phase_1_time)
            else:
                phase_1_times_listener.append(phase_1_time)

            if 'stop_date' not in sync:
                import sys
                print(f'Missing enclosing event for the second phase of a sync {sync_id:d}', file=sys.stderr)
                continue

            start_date = end_date
            end_date = sync['stop_date']
            phase_2_time = diff_in_seconds(start_date, end_date)

            phase_2_times.append(phase_2_time)

            if ending_event == sync_event_name:
                phase_2_times_sync.append(phase_2_time)
            else:
                phase_2_times_listener.append(phase_2_time)

        reporter(phase_1_times, 'phase_1_times')
        reporter(phase_2_times, 'phase_2_times')
        reporter(phase_1_times_sync, 'sync_phase_1_times')
        reporter(phase_2_times_sync, 'sync_phase_2_times')
        reporter(phase_1_times_listener, 'listener_phase_1_times')
        reporter(phase_2_times_listener, 'listener_phase_2_times')

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

        - learn_level_quorum: time between learning about a new level and learning (2/3)N prime units at this level

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

        - send_poset_info_fail: ONE NUMBER: total number of failed invocations of send_poset_info

        - send_units_fail: ONE NUMBER: total number of failed invocations of send_units

        - send_requests_fail: ONE NUMBER: total number of failed invocations of send_requests

        - bytes_sent_per_sync: bytes sent by invocations of sync

        - bytes_sent_per_sec: time series describing amount of bytes sent per second by invocations of sync

        - bytes_received_per_sec: time series describing amount of bytes received per second by invocations of sync

        - time_send_poset: time spent in sync by invocations of send_poset_info

        - time_send_units: time spent in sync by invocations of send_units

        - time_send_requests: time spent in sync by invocations of send_requests

        - phase_1_times: time spent in sync between its start and end of a first invocation of receive_poset_info phase (reverse
          order in case of listener events)

        - phase_2_times: time spent in sync between end of a first invocation of receive_poset_info (or send_poset_info in case
          of a listener) and end of a sync

        - sync_phase_1_times: analogous to phase_1_times but limited to only sync events (syncs started by this process)

        - sync_phase_2_times: analogous to phase_2_times but limited to only sync events (syncs started by this process)

        - listener_phase_1_times: analogous to phase_1_times but limited to only listener sync events (syncs started not
          by this process)

        - process) listener_phase_2_times: analogous to phase_2_times but limited to only listener sync events (syncs started
          not by this process)

        - create_delay: the difference in time between two consecutive create_unit

        - sync_delay: the difference in time between two consecutive synchronization attempts

        - n_recv_syncs: the number of simultaneous incoming connections (sampled when a new process is trying to connect)

        - memory_MiB: total memory used by the process

        - add_unit_time_s: average time spend on adding one unit to the poset

        - time_compress: time (per sync) spent on compressing the msgs sent through sockets

        - time_decompress: time (per sync) spent on decompressing the msgs sent through sockets

        - time_pickle: time (per sync) spent on pickling the msgs sent through sockets

        - time_unpickle: time (per sync) spent on unpickling the msgs sent through sockets

        - time_verify: time (per sync) spent on verifying the units received

        - time_add_units: time (per sync) spent on adding to poset the units received

        - time_cpu_sync: total cpu time (per sync), i.e. (de)compress + (un)pickle + verify + add_units

        - time_order: cpu time spent on ordering units (per timing unit), includes dealing with txs

        - time_create: cpu time to create a unit

        - time_decision: cpu time spent on attempting to decide popularity after adding a new prime unit

        - n_parents: number of parents of units created by this process

        - n_maximal: number of maximal units in the poset just before creating a unit

        - n_create_fail: ONE NUMBER: how many times the create failed (no choice of appropriate parents)

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
        levels, n_units_per_level, levels_plus_decided, levels_poset_plus_decided, level_delays, n_txs_per_level = self.get_timing_decision_stats()
        _append_stat_line(n_units_per_level, 'n_units_decision')
        _append_stat_line(level_delays, 'time_decision')
        _append_stat_line(levels_plus_decided, 'decision_height')
        _append_stat_line(levels_poset_plus_decided, 'poset_decision_height')
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

        # delay between learning about a new level and learning (2/3)N prime units at this level
        data = self.get_delays_learn_prime_quorum()
        _append_stat_line(data, 'learn_level_quorum')

        # info about syncs
        sync_plot_file = os.path.join(dest_dir,'plot-dot-sync', 'dot-sync-' + str(self.process_id) + '.png')
        os.makedirs(os.path.dirname(sync_plot_file), exist_ok=True)

        sent_per_sync, recv_per_sync, time_per_sync, time_per_unit_ex, \
            bytes_per_unit_ex, est_conn_time, syncs_not_succ, send_poset_info_not_succeeded, \
            send_units_not_succeeded, send_requests_not_succeeded, bytes_sent_per_sync = self.get_sync_info(sync_plot_file)
        _append_stat_line(sent_per_sync, 'units_sent_sync')
        _append_stat_line(recv_per_sync, 'units_recv_sync')
        _append_stat_line(time_per_sync, 'time_per_sync')
        _append_stat_line(time_per_unit_ex, 'time_per_unit_ex')
        _append_stat_line(bytes_per_unit_ex, 'bytes_per_unit_ex')
        _append_stat_line(est_conn_time, 'est_conn_time')
        _append_stat_line([syncs_not_succ], 'sync_fail')
        _append_stat_line([send_poset_info_not_succeeded], 'send_poset_info_fail')
        _append_stat_line([send_units_not_succeeded], 'send_units_fail')
        _append_stat_line([send_requests_not_succeeded], 'send_requests_fail')
        _append_stat_line(bytes_sent_per_sync, 'bytes_sent_per_sync')
        _append_stat_line(self.build_bytes_per_second_stats(self.get_outbound_network_events()), 'bytes_sent_per_sec')
        _append_stat_line(self.build_bytes_per_second_stats(self.get_inbound_network_events()), 'bytes_received_per_sec')

        # info about rounds of syncs
        events_per_sync = [s.get('events', []) for _, s in self.syncs.items()]

        send_poset_info_events = [[a for a in events if a['event_name'] == 'send_poset_info'] for events in events_per_sync]
        send_poset_info_times = [self.get_event_time(evs) for evs in send_poset_info_events]
        _append_stat_line(send_poset_info_times, 'time_send_poset')

        send_units_events = [[a for a in events if a['event_name'] == 'send_units'] for events in events_per_sync]
        send_units_times = [self.get_event_time(evs) for evs in send_units_events]
        _append_stat_line(send_units_times, 'time_send_units')

        send_requests_events = [[a for a in events if a['event_name'] == 'send_requests'] for events in events_per_sync]
        send_requests_times = [self.get_event_time(evs) for evs in send_requests_events]
        _append_stat_line(send_requests_times, 'time_send_requests')

        # info about phases of a sync
        self.prepare_phases_report(_append_stat_line)

        # gen plot on units exchanged vs time
        units_ex_plot_file = os.path.join(dest_dir, 'plot-units', f'units-{self.process_id}.png')
        os.makedirs(os.path.dirname(units_ex_plot_file), exist_ok=True)
        self.gen_units_exchanged_plots(units_ex_plot_file)

        # delay stats
        create_delays, sync_delays = self.get_delay_stats()
        _append_stat_line(create_delays, 'create_delay')
        _append_stat_line(sync_delays, 'sync_delay')

        # number of concurrent received syncs
        data = self.current_recv_sync_no
        _append_stat_line(data, 'n_recv_syncs')

        # memory
        mem_plot_file = os.path.join(dest_dir, 'plot-mem', 'mem-' + str(self.process_id) + '.png')
        os.makedirs(os.path.dirname(mem_plot_file), exist_ok=True)

        data = self.get_memory_usage_vs_poset_size(mem_plot_file)
        _append_stat_line(data, 'memory_MiB')


        # cpu time
        sync_bar_plot_file_cpu = os.path.join(dest_dir,'plot-sync-bar','cpu-sync-' + str(self.process_id) + '.png')
        sync_bar_plot_file_all = os.path.join(dest_dir,'plot-sync-bar','all-sync-' + str(self.process_id) + '.png')
        sync_bar_plot_cpu_network_rest = os.path.join(dest_dir,'plot-sync-bar','cpu-network-rest-sync-' + str(self.process_id) + '.png')
        os.makedirs(os.path.dirname(sync_bar_plot_file_cpu), exist_ok=True)
        times = self.get_cpu_times(sync_bar_plot_file_cpu,
                                   sync_bar_plot_file_all,
                                   sync_bar_plot_cpu_network_rest)
        _append_stat_line(times['t_prepare_units'], 'time_prepare')
        _append_stat_line(times['t_pickle_units'], 'time_pickle')
        _append_stat_line(times['t_unpickle_units'], 'time_unpickle')
        _append_stat_line(times['t_verify_signatures'], 'time_verify')
        _append_stat_line(times['t_add_units'], 'time_add_units')
        _append_stat_line(times['t_tot_sync'], 'time_cpu_sync')
        _append_stat_line(times['t_order_level'], 'time_order')
        _append_stat_line(times['t_create'], 'time_create')
        _append_stat_line(times['t_attempt_timing'], 'time_decision')

        # n_parents
        _append_stat_line(self.get_n_parents(), 'n_parents')

        # n_maximal
        _append_stat_line(self.max_units_cnts, 'n_maximal')

        # n_create_fail
        _append_stat_line([self.n_create_fail], 'n_create_fail')

        # plot network statistics
        bar_plot_network_outbound = os.path.join(dest_dir, 'plot-network-bar','network-outbound-' + str(self.process_id) + '.png')
        bar_plot_network_inbound = os.path.join(dest_dir, 'plot-network-bar','network-inbound-' + str(self.process_id) + '.png')
        os.makedirs(os.path.dirname(bar_plot_network_outbound), exist_ok=True)
        self.plot_network_utilization(bar_plot_network_outbound, bar_plot_network_inbound)

        report_file = os.path.join(dest_dir, 'txt-basic', file_name_prefix+str(self.process_id)+'.txt')
        os.makedirs(os.path.dirname(report_file), exist_ok=True)

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


def format_line(field_list, data=None):
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

        just_len = 25 if field == 'name' else len(field) + 15
        entry = entry.ljust(just_len)
        line += entry
    return line


# ------------------ Helper Functions for parser ------------------

def get_tokens(space_separated_string):
    return [s.strip() for s in space_separated_string.split()]


def parse_unit_list(space_separated_units):
    return [s[1:-1] for s in space_separated_units.split()]
