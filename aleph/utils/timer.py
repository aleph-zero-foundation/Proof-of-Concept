import gc
from logging import Logger
from time import perf_counter as get_time


class timer:
    """
    A simple timer for small snippets of code, usable as a context manager.

    Usage:

        with timer('group', 'somename'):
            code1
            code2
            code3

        with timer('group', 'somename', disable_gc=True): #disables garbage collector in the whole block
            code1
            ...

        timer.write_summary(where, groups)
            #*where* can be a Logger instance, None (stdout - default) or any object with callable write() attribute
            #*groups* - print summary only for chosen groups. By default prints all groups

        timer.reset(group)
            #forgets about everything that was recorded with timers from a given group. If *group* is None, forgets everything
    """

    results = {}

    def __init__(self, group, name, disable_gc=True):
        self.group = group
        self.name = name
        self.disable_gc = disable_gc


    def __enter__(self):
        if self.disable_gc:
            self.old_gc = gc.isenabled()
            gc.disable()
        self.start = get_time()
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        end = get_time()
        if self.disable_gc and self.old_gc:
            gc.enable()
        if self.group not in self.results:
            self.results[self.group] = {}
        g = self.results[self.group]
        if self.name not in g:
            g[self.name] = 0.0
        g[self.name] += end - self.start


    @classmethod
    def write_summary(cls, where=None, groups=None):
        if where is None:
            write = print
        elif isinstance(where, Logger):
            write = where.info
        elif hasattr(where, 'write') and callable(where.write):
            write = where.write

        groups = groups or list(sorted(cls.results.keys()))

        for group in groups:
            if group in cls.results:
                for name, time in cls.results[group].items():
                    write(f'timer  {str(group)}  |  {name:20}  took  {time:10.6f}  seconds')


    @classmethod
    def reset(cls, group=None):
        if group is None:
            cls.results = {}
        elif group in cls.results:
            del cls.results[group]

