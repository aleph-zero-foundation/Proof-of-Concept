'''This module implements a poset with a modified consensus rule (as compared to the whitepaper from Nov 2018).'''

from aleph.data_structures import Poset




class FastPoset(Poset):
	def __init__(self, n_processes, crp = None, compliance_rules = None, memo_height = 10, use_tcoin = False, process_id = None):
        '''
        :param int n_processes: the committee size
        :param list compliance_rules: dictionary string -> bool
        '''
        self.n_processes = n_processes
        self.default_compliance_rules = {'forker_muting': True, 'parent_diversity': True, 'growth': True, 'threshold_coin': use_tcoin}
        self.compliance_rules = compliance_rules
        self.use_tcoin = use_tcoin
        # process_id is used only to support tcoin (i.e. in case use_tcoin = True), to know which shares to add and which tcoin to pick from dealing units
        self.process_id = process_id

        self.units = {}
        self.max_units_per_process = [[] for _ in range(n_processes)]
        self.forking_height = [float('inf')] * n_processes

        #common random permutation
        self.crp = crp

        self.level_reached = 0
        self.level_timing_established = 0
        # threshold coins dealt by each process; initialized from dealing units
        self.threshold_coins = [[] for _ in range(n_processes)]

        self.prime_units_by_level = {}

        # The list of dealing units for every process -- in a healthy situation (absence of forkers) there should be one per process
        self.dealing_units = [[] for _ in range(n_processes)]

        #timing units
        self.timing_units = []

        #a structure for efficiently executing  the units_by_height method, for every process this is a sparse list of units sorted by height
        #every unit of height memo_height*k for some k>=0 is memoized, in case of forks only one unit is added
        self.memoized_units = [[] for _ in range(n_processes)]
        self.memo_height = memo_height

        #a structure for memoizing partial results about the computation of pi/delta
        # it has the form of a dict with keys being unit hashes (U_c.hash) and values being dicts indexed by pairs (fun, U.hash)
        # whose value is the memoized value of computing fun(U_c, U) where fun in {pi, delta}
        self.timing_partial_results = {}



    def level(self, U):
        '''
        Calculates the level in the poset of the unit U.
        :param unit U: the unit whose level is being requested
        '''
        # TODO: so far this is a rather naive implementation -- loops over all prime units at level just below U

        if len(U.parents) == 0:
            return 0

        if U.level is not None:
            return U.level

        # let m be the max level of U's parents
        m = max([self.level(V) for V in U.parents])
        # now, the level of U is either m or (m+1)

        # need to count all processes that produced a unit V of level m such that U'<=U
        # we can limit ourselves to prime units V
        processes_below = 0

        for process_id in range(self.n_processes):
            Vs = self.prime_units_by_level[m][process_id]
            if any(self.below(V, U) for V in Vs):
                processes_below += 1

            # For efficiency:
            if 3*(processes_below + self.n_processes - 1 - process_id) < 2*self.n_processes:
                break

        # same as (...)>=2/3*(...) but avoids floating point division
        U.level = m+1 if 3*processes_below >= 2*self.n_processes else m
        return U.level

    def proves_popularity(self, V, U_c):
    	'''
    	Checks whether V proves that U_c is popular on V's level, i.e. whether there exist
    	       >=2/3 N prime units W on level L(V)-1 such that: (1) W <= V and (2) U_c <= W.
    	:param unit V: the "prover" unit
    	:param unit U_c: the unit tested for popularity
    	:returns: True or False: does V prove that U_c is popular?
    	'''
    	level_V = self.level(V)
    	if level_V == 0:
    		return False

    	vouchers = 0
    	for Ws in self.prime_units_by_level[level_V - 1]:
    		# Ws is typically a list of *one* unit, but in case of forks can be longer, hence the use of "any"
    		if any(self.below(U_c, W) and self.below(W, V) for W in Ws):
    			vouchers += 1

    	return 3*vouchers >= 2*self.n_processes
