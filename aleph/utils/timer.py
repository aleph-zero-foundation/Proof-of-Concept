import gc
from logging import Logger
from time import perf_counter as get_time

class timer:
    """
    A simple timer for small snippets of code, usable as a context manager.

    Usage:

        with timer('somename'):
            code1
            code2
            code3

        with timer('somename', disable_gc=True): #disables garbage collector in the whole block
            code1
            ...

        timer.write_summary(where, names)
            #*where* can be a Logger instance, None (stdout - default) or any object with callable write() attribute
            #*names* - print summary only for chosen names. By default prints all names

        timer.reset(name)
            #forgets about everything that happened for timer *name*. If *name* is None, forgets everything
    """

    results = {}

    def __init__(self, name, disable_gc=False):
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
        if self.name not in self.results:
            self.results[self.name] = []
        self.results[self.name].append(end - self.start)


    @classmethod
    def write_summary(cls, where=None, names=None):
        if where is None:
            write = print
        elif isinstance(where, Logger):
            write = where.info
        elif hasattr(where, 'write'):
            write = where.write

        names = names or list(sorted(cls.results.keys()))

        for name in names:
            for i, time in enumerate(cls.results[name]):
                write('Timer  {} ({:3})  took  {:8.5f}  seconds'.format(name, i+1, time))


    @classmethod
    def reset(cls, name=None):
        if name is None:
            cls.results = {}
        elif name in cls.results:
            del cls.results[name]

