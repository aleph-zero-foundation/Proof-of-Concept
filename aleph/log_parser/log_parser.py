from parse import parse, compile

pattern =  compile("[{date}] [{level}] [{name}] {msg} [{file_line}]")
split_on_bar = compile("{left}|{right}")


def get_tokens(space_separated_string)
    return [s.strip() for s in space_separated_string.split()]


def event_from_log_line(line):
    res =  pattern.parse(line)
    assert res is not None
    event = res.named

    msg = event['msg']
    res = split_on_bar.parse(msg)
    assert res is not None, "split on bar failed"
    event_type, body = res.named






    return event


def parse_log_file(file_path):
    cnt = 0
    with open(file_path, "r") as log_file:
        for line in log_file:
            cnt += 1
            e = event_from_log_line(line.strip())
            if cnt == 1000:
                print(e)

    print(cnt)


path = r'../../tests/aleph.log'
parse_log_file(path)