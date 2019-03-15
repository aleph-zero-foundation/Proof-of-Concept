N_PARENTS             = 10                  # maximal number of parents a unit can have

CREATE_DELAY          = 2.0                 # delay after creating a new unit
STEP_SIZE             = 0.14                # a number in (0,1) describing how aggresive is the create_delay adjusting mechanism, large = aggresive

SYNC_INIT_DELAY       = 0.015625            # delay after initianing a sync with other processes


N_RECV_SYNC           = 10                  # number of allowed parallel received syncs
N_INIT_SYNC           = 10                  # number of allowed parallel initiated syncs

TXPU                  = 1                   # number of transactions per unit
TX_LIMIT              = 1000000             # limit of all txs generated for one process

LEVEL_LIMIT           = 20                  # maximal level after which process shuts down
UNITS_LIMIT           = None                # maximal number of units that are constructed
SYNCS_LIMIT           = None                # maximal number of syncs that are performed

USE_TCOIN             = 1                   # whether to use threshold coin
SMART_CREATE          = 0                   # use the create strategy where we wait until 2/3N prime units are available, and use create_unit_greedy
USE_MAX_PARENTS       = 1                   # prefer maximal units (globally maximal in poset) when choosing parents
PRECOMPUTE_POPULARITY = 0                   # precompute popularity proof to ease computational load of Poset.compute_vote procedure
ADAPTIVE_DELAY        = 1                   # whether to use the adaptive strategy of determining create_delay

VOTING_LEVEL          = 3                   # level at which the first voting round occurs, this is "t" from the write-up
PI_DELTA_LEVEL        = 12                  # level at which to switch from the "fast" to the pi_delta algorithm
ADD_SHARES            = PI_DELTA_LEVEL - 1  # level at which to start adding coin shares to units, it's safe to make it PI_DELTA_LEVEL - 1

HOST_IP               = '127.0.0.1'         # default ip address of a process
HOST_PORT             = 8888                # default port of incoming syncs

LOGGER_NAME           = 'aleph'             # name of our logger and logfile
TX_SOURCE             = 'tx_source_gen'     # source of txs
