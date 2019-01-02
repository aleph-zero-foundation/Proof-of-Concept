N_PARENTS = 2
N_CREATE = 2  # number of new units per second
CREATE_FREQ = 1/N_CREATE  # frequency of creating new units
SYNC_INIT_FREQ = CREATE_FREQ # frequency of initianing syncs with other processes
SIGNING_FUNCTION = None  # TODO pick one!
N_TXS = 10000  # number of transactions per unit

HOST_IP = '127.0.0.1'
HOST_PORT = 8888

N_RECV_SYNC = 5
LOGGER_NAME = 'aleph'

ADD_SHARES = 6

import logging

log_format = '[%(asctime)s] [%(levelname)-8s] [%(name)-10s] %(message)s [%(filename)s:%(lineno)d]'
logging.basicConfig(filename='other.log',
                    level=logging.DEBUG,
                    format=log_format,
                    filemode='w')
logger = logging.getLogger(LOGGER_NAME)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('aleph.log', mode='w')
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter(log_format))
logger.addHandler(fh)
