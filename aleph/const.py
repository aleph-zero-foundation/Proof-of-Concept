N_PARENTS       = 2            # maximal number of parents a unit can have
USE_MAX_PARENTS = 1            # prefer maximal units (globally maximal in poset) when choosing parents

USE_TCOIN       = 0            # whether to use threshold coin
ADD_SHARES      = 4            # level at which start to adding coin shares to units

CREATE_FREQ     = 1.0          # frequency of creating new units
SYNC_INIT_FREQ  = 0.5          # frequency of initianing syncs with other processes
N_RECV_SYNC     = 10           # number of allowed parallel syncs

TXPU            = 2000         # number of transactions per unit
TX_LIMIT        = 1000000

LEVEL_LIMIT     = 100          # maximal level after which process shuts donw
UNITS_LIMIT     = None         # maximal number of units that are constructed
SYNCS_LIMIT     = None         # maximal number of syncs that are performed

HOST_IP         = '127.0.0.1'  # default ip address of a process
HOST_PORT       = 8888         # default port of incoming syncs

LOGGER_NAME     = 'aleph'

USE_FAST_POSET  = 1            # use fast poset in place of poset when using Processes

TX_SOURCE       = 'tx_source_gen'
