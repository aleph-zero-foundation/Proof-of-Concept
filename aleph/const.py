N_PARENTS       = 2            # maximal number of parents a unit can have
USE_MAX_PARENTS = 1            # prefer maximal units (globally maximal in poset) when choosing parents

VOTING_LEVEL    = 3            # level at which the first voting round occurs, this is "t" from the write-up
PI_DELTA_LEVEL  = 12           # level at which to switch from the "fast" to the pi_delta algorithm
USE_TCOIN       = 0            # whether to use threshold coin
ADD_SHARES      = PI_DELTA_LEVEL - 1   # level at which to start adding coin shares to units, it's safe to make it PI_DELTA_LEVEL - 1
                                       # keeping it here for the purpose of tests, for which we might want to set it to a lower value

CREATE_FREQ     = 1.0          # frequency of creating new units
SYNC_INIT_FREQ  = 1.0          # frequency of initianing syncs with other processes
N_RECV_SYNC     = 10           # number of allowed parallel received syncs
N_INIT_SYNC     = 5            # number of allowed parallel initiated syncs

TXPU            = 2000         # number of transactions per unit
TX_LIMIT        = 1000000

LEVEL_LIMIT     = 100          # maximal level after which process shuts down
UNITS_LIMIT     = None         # maximal number of units that are constructed
SYNCS_LIMIT     = None         # maximal number of syncs that are performed

HOST_IP         = '127.0.0.1'  # default ip address of a process
HOST_PORT       = 8888         # default port of incoming syncs

LOGGER_NAME     = 'aleph'

USE_FAST_POSET  = 1            # use fast poset in place of poset when using Processes

TX_SOURCE       = 'tx_source_gen'
