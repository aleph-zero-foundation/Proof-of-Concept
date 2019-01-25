from parse import parse, compile
from datetime import datetime




class LogAnalyzer:
    '''
    A class for producing statistics about the protocol execution given logged events.
    '''
    def __init__(self, file_path):
        self.units = {}
        self.file_path = file_path

    def handle_event(self, event):
        ev_type = event['type']
        if ev_type == 'create_add':
            assert event['units'] not in self.units, "Unit hash collision?"
            U = event['units']
            self.units[U] = {'created': event['date']}

    def analyze(self):
        log_parser = LogParser(self.file_path)
        for event in log_parser.get_events():
            print(event)







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

    def parse_add_lin_order(self, ev_type, ev_params, msg_body, event):
        pattern_add_line = compile("Added {n_units:d} units to the linear order {unit_list}")
        parsed = pattern_add_line.parse(msg_body)
        event['n_units'] = parsed['n_units']
        event['units'] = parse_unit_list(parsed['unit_list'])
        assert event['n_units'] == len(event['units'])


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
analyzer = LogAnalyzer(path)
analyzer.analyze()