from parse import parse, compile
from datetime import datetime

pattern =  compile("[{date}] [{level}] [{name}] {msg} [{file_line}]")
split_on_bar = compile("{left}|{right}")


def get_tokens(space_separated_string):
    return [s.strip() for s in space_separated_string.split()]

def parse_unit_list(space_separated_units):
    return [s[1:-1] for s in space_separated_units.split()]


def parse_create(ev_type, ev_params, msg_body, event):
    event['process_id'] = int(ev_params[0])
    pattern_create = compile("Created a new unit <{unit}>")
    event['units'] = pattern_create.search(msg_body)['unit']

def parse_mem_usg(ev_type, ev_params, msg_body, event):
    event['process_id'] = int(ev_params[0])
    pattern_memory = compile("{usage:d} MiB")
    event['memory'] = pattern_memory.search(msg_body)['usage']

def parse_add_lin_order(ev_type, ev_params, msg_body, event):
    event['process_id'] = int(ev_params[0])
    pattern_add_line = compile("Added {n_units:d} units to the linear order {unit_list}")
    parsed = pattern_memory.parse(msg_body)
    event['n_units'] = parsed['n_units']
    event['units'] = parse_unit_list(parsed['unit_list'])
    assert event['n_units'] == len(event['units'])



def event_from_log_line(line):
    parsed_line =  pattern.parse(line)
    assert parsed_line is not None
    event = parsed_line.named

    msg = event['msg']
    split_msg = split_on_bar.parse(msg)
    if split_msg is None:
        # this happens when some log message is not formatted using "|"
        # it means we should skip it
        return None
    event_descr, msg_body = split_msg['left'].strip(), split_msg['right'].strip()
    #print(event_descr)

    ev_tokens = get_tokens(event_descr)
    ev_type = ev_tokens[0]

    event_handler = {
        'create_add': parse_create,
        'memory_usage': parse_mem_usg,
        }
    if ev_type in event_handler:
        event_handler[ev_type](ev_type, ev_tokens[1:], msg_body, event)
    else:
        return None
    event.pop('msg', None)
    event.pop('level', None)
    event.pop('name', None)
    event['date'] = datetime.strptime(event['date'], "%Y-%m-%d %H:%M:%S,%f")
    return event




def parse_log_file(file_path):
    cnt = 0
    evs = []
    with open(file_path, "r") as log_file:
        for line in log_file:
            cnt += 1
            e = event_from_log_line(line.strip())
            if e is not None:
                evs.append(e)
            #if cnt == 1000:
            #    print(e)
    for e in evs:
        print(e)
    #print(cnt)


path = r'../../tests/aleph.log'
parse_log_file(path)