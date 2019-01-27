from parse import parse, compile
from datetime import datetime





class LogParser:

    def __init__(self, file_path):
        self.msg_pattern =  compile("[{date}] [{msg_level}] [{name}] {msg} [{file_line}]")
        self.split_on_bar = compile("{left}|{right}")
        self.file_path = file_path

        # initialize a bunch of parsing patterns to speed up parsing
        self.pattern_create = compile("Created a new unit <{unit}>")
        self.pattern_memory = compile("{usage:d} MiB")
        self.pattern_level = compile("Level {level:d} reached")
        self.pattern_add_foreign = compile("trying to add <{unit}> from {ex_id:d} to poset")
        self.pattern_add_line = compile("At lvl {timing_level:d} added {n_units:d} units to the linear order {unit_list}")
        self.pattern_decide_timing = compile("Timing unit for lvl {level:d} decided at lvl + {plus_level:d}")
        self.pattern_sync_establish = compile("Established connection to {process_id:d}")
        self.pattern_listener_succ = compile("Syncing with {process_id:d} succesful")
        self.pattern_receive_units_done = compile("Received {n_bytes:d} bytes and {n_units:d} units")
        self.pattern_send_units_sent = compile("Sent {n_units:d} units and {n_bytes:d} bytes to {ex_id:d}")
        self.pattern_try_sync = compile("Establishing connection to {target:d}")
        self.pattern_listener_sync_no = compile("Number of syncs is {n_recv_syncs:d}")

    def parse_create(self, ev_type, ev_params, msg_body, event):
        event['units'] = self.pattern_create.parse(msg_body)['unit']

    def parse_mem_usg(self, ev_type, ev_params, msg_body, event):
        event['memory'] = self.pattern_memory.parse(msg_body)['usage']

    def parse_new_level(self, ev_type, ev_params, msg_body, event):
        event['level'] = self.pattern_level.parse(msg_body)['level']

    def parse_listener_succ(self, ev_type, ev_params, msg_body, event):
        parsed = self.pattern_listener_succ.parse(msg_body)
        event['sync_id'] = int(ev_params[1])
        event['target'] = parsed['process_id']
        event['type'] = 'sync_success'

    def parse_try_sync(self, ev_type, ev_params, msg_body, event):
        parsed = self.pattern_try_sync.parse(msg_body)
        event['sync_id'] = int(ev_params[1])
        event['target'] = parsed['target']
        event['type'] = 'try_sync'

    def parse_listener_sync_no(self, ev_type, ev_params, msg_body, event):
        parsed = self.pattern_listener_sync_no.parse(msg_body)
        event['n_recv_syncs'] = parsed['n_recv_syncs']

    def parse_establish(self, ev_type, ev_params, msg_body, event):
        if ev_type == 'listener_establish':
            event['mode'] = 'listen'
        else:
            event['mode'] = 'sync'
            parsed = self.pattern_sync_establish.parse(msg_body)
            event['target'] = parsed['process_id']
        event['sync_id'] = int(ev_params[1])
        event['type'] = 'sync_establish'

    def parse_add_foreign(self, ev_type, ev_params, msg_body, event):
        event['sync_id'] = int(ev_params[1])
        event['units'] = self.pattern_add_foreign.parse(msg_body)['unit']

    def parse_add_lin_order(self, ev_type, ev_params, msg_body, event):
        parsed = self.pattern_add_line.parse(msg_body)
        event['n_units'] = parsed['n_units']
        event['units'] = parse_unit_list(parsed['unit_list'])
        event['level'] = parsed['timing_level']
        assert event['n_units'] == len(event['units'])

    def parse_decide_timing(self, ev_type, ev_params, msg_body, event):
        parsed = self.pattern_decide_timing.parse(msg_body)
        event['level'] = parsed['level']
        event['timing_decided_level'] = parsed['level'] + parsed['plus_level']

    def parse_receive_units_done(self, ev_type, ev_params, msg_body, event):
        parsed = self.pattern_receive_units_done.parse(msg_body)
        event['n_bytes'] = parsed['n_bytes']
        event['n_units'] = parsed['n_units']
        event['sync_id'] = int(ev_params[1])
        event['type'] = 'receive_units'

    def parse_send_units_sent(self, ev_type, ev_params, msg_body, event):
        parsed = self.pattern_send_units_sent.parse(msg_body)
        event['n_bytes'] = parsed['n_bytes']
        event['n_units'] = parsed['n_units']
        event['sync_id'] = int(ev_params[1])
        event['type'] = 'send_units'



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
        ev_tokens = get_tokens(event_descr)

        ev_type = ev_tokens[0]

        parse_mapping = {
            'create_add' : self.parse_create,
            'memory_usage' : self.parse_mem_usg,
            'add_linear_order' : self.parse_add_lin_order,
            'new_level' : self.parse_new_level,
            'add_foreign' : self.parse_add_foreign,
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
            'listener_sync_no' : self.parse_listener_sync_no
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




# ------------------ Helper Functions for the Log Parser ------------------

def get_tokens(space_separated_string):
    return [s.strip() for s in space_separated_string.split()]

def parse_unit_list(space_separated_units):
    return [s[1:-1] for s in space_separated_units.split()]




