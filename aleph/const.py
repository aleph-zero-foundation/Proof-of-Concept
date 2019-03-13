N_PARENTS       = 10           # maximal number of parents a unit can have
USE_MAX_PARENTS = 1            # prefer maximal units (globally maximal in poset) when choosing parents

VOTING_LEVEL    = 3            # level at which the first voting round occurs, this is "t" from the write-up
PI_DELTA_LEVEL  = 12           # level at which to switch from the "fast" to the pi_delta algorithm
USE_TCOIN       = 1            # whether to use threshold coin
ADD_SHARES      = PI_DELTA_LEVEL - 1   # level at which to start adding coin shares to units, it's safe to make it PI_DELTA_LEVEL - 1
                                       # keeping it here for the purpose of tests, for which we might want to set it to a lower value

CREATE_DELAY    = 0.5          # delay after creating a new unit
SYNC_INIT_DELAY = 0.1          # delay after initianing a sync with other processes
N_RECV_SYNC     = 10           # number of allowed parallel received syncs
N_INIT_SYNC     = 10           # number of allowed parallel initiated syncs

SMART_CREATE    = 1            # use the create strategy where we wait until 2/3N prime units are available, and use create_unit_greedy

TXPU            = 10           # number of transactions per unit
TX_LIMIT        = 1000000

LEVEL_LIMIT     = 20           # maximal level after which process shuts down
UNITS_LIMIT     = None         # maximal number of units that are constructed
SYNCS_LIMIT     = None         # maximal number of syncs that are performed

HOST_IP         = '127.0.0.1'  # default ip address of a process
HOST_PORT       = 8888         # default port of incoming syncs

LOGGER_NAME     = 'aleph'

TX_SOURCE       = 'tx_source_gen'
