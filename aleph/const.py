N_PARENTS       = 2

USE_TCOIN       = False        # whether to use threshold coin
ADD_SHARES      = 4            # level at which start to adding coin shares to units

CREATE_FREQ     = 1            # frequency of creating new units
SYNC_INIT_FREQ  = 1            # frequency of initianing syncs with other processes
N_RECV_SYNC     = 10           # number of allowed parallel syncs

TXPU            = 2000         # number of transactions per unit

LEVEL_LIMIT     = None         # maximal level after which process shuts donw
UNITS_LIMIT     = None         # maximal number of units that are constructed
SYNCS_LIMIT     = None         # maximal number of syncs that are performed

HOST_IP         = '127.0.0.1'  # default ip address of a process
HOST_PORT       = 8888         # default port of incoming syncs

LOGGER_NAME     = 'aleph'

SEND_COMPRESSED = True         # use zlib compressing/decompressing when sending data over the network
