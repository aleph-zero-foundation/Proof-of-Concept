'''This module implements a poset with a modified consensus rule (as compared to the whitepaper from Nov 2018).'''

import logging

from aleph.data_structures import Poset
import aleph.const as consts




class FastPoset(Poset):
    '''
    An alternative instantiation of Poset -- with different consensus rules.
    '''
    def __init__(self, n_processes, process_id = None, crp = None, use_tcoin = consts.USE_TCOIN,
                compliance_rules = None, memo_height = 10, consensus_params = None):
        '''
        :param int n_processes: the committee size
        :param list compliance_rules: dictionary string -> bool
        '''
        self.n_processes = n_processes
        self.default_compliance_rules = {'forker_muting': True, 'parent_diversity': False, 'growth': False, 'expand_primes': True, 'threshold_coin': use_tcoin}
        self.compliance_rules = compliance_rules
        self.use_tcoin = use_tcoin
        # process_id is used only to support tcoin (i.e. in case use_tcoin = True), to know which shares to add and which tcoin to pick from dealing units
        self.process_id = process_id

        self.units = {}
        self.max_units_per_process = [[] for _ in range(n_processes)]
        # the set of globally maximal units in the poset
        self.max_units = set()
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


        default_consensus_params = {'t_first_vote' : 4, 't_switch_to_pi_delta' : 123456789}
        self.consensus_params = default_consensus_params if consensus_params is None else consensus_params

    def add_unit(self, U):
        '''
        Add a unit compliant with the rules, what was checked by check_compliance.
        This method does the following:
            0. add the unit U to the poset
            1. if it is a dealing unit, add it to self.dealing_units
            2. update the lists of maximal elements in the poset.
            3. update forking_height
            4. if U is prime, add it to prime_units_by_level
            5. set ceil attribute of U and update ceil of predecessors of U
            6. if required, adds U to memoized_units
        :param unit U: unit to be added to the poset
        '''

        # 0. add the unit U to the poset
        assert U.level is not None, "Level of the unit being added is not computed."

        self.level_reached = max(self.level_reached, U.level)
        self.units[U.hash()] = U

        # if it is a dealing unit, add it to self.dealing_units
        if not U.parents and not U in self.dealing_units[U.creator_id]:
            self.dealing_units[U.creator_id].append(U)
            # extract the corresponding tcoin black box (this requires knowing the process_id)
            if self.use_tcoin:
                assert self.process_id is not None, "Usage of tcoin enable but process_id not set."
                self.extract_tcoin_from_dealing_unit(U, self.process_id)


        # 2. updates the lists of maximal elements in the poset and forkinf height
        if len(U.parents) == 0:
            assert self.max_units_per_process[U.creator_id] == [], "A second dealing unit is attempted to be added to the poset"
            self.max_units_per_process[U.creator_id] = [U]
            self.max_units.add(U)
        else:
            # from max_units remove the ones that are U's parents, and add U as a new maximal unit
            self.max_units = self.max_units - set(U.parents)
            self.max_units.add(U)

            if U.self_predecessor in self.max_units_per_process[U.creator_id]:
                self.max_units_per_process[U.creator_id].remove(U.self_predecessor)
                self.max_units_per_process[U.creator_id].append(U)
            else:
                # 3. update forking_height
                self.max_units_per_process[U.creator_id].append(U)
                self.forking_height[U.creator_id] = min(self.forking_height[U.creator_id], U.height)

        # 4. if U is prime, update prime_units_by_level
        if self.is_prime(U):
            if U.level not in self.prime_units_by_level:
                self.prime_units_by_level[U.level] = [[] for _ in range(self.n_processes)]
            self.prime_units_by_level[U.level][U.creator_id].append(U)

        # 6. Update memoized_units
        if U.height % self.memo_height == 0:
            n_units_memoized = len(self.memoized_units[U.creator_id])
            U_no = U.height//self.memo_height
            if n_units_memoized >= U_no + 1:
                #this means that U.creator_id is forking and there is already a unit added on this height
                pass
            else:
                assert n_units_memoized == U_no, f"The number of units memoized is {n_units_memoized} while it should be {U_no}."
                self.memoized_units[U.creator_id].append(U)



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
            >=2/3 N units W on level L(V)-1 such that: (1) W <= V and (2) U_c <= W.
        :param unit V: the "prover" unit
        :param unit U_c: the unit tested for popularity
        :returns: True or False: does V prove that U_c is popular?
        '''
        U_c_hash, V_hash = U_c.hash(), V.hash()
        memo = self.timing_partial_results[U_c_hash]
        if ('proof', V_hash) in memo:
            return memo[('proof', V_hash)]

        level_V = self.level(V)
        if level_V <= U_c.level or not self.below(U_c, V):
            return False

        # implementation of a simple DFS from V down until we hit units of level (level_V - 2)
        threshold = (2*self.n_processes + 2)//3
        seen_units = set([V])
        seen_processes = set()
        stack = [V]
        # the invariants here are that all elements W on stack:
        #    (1) are also in seen_units
        #    (2) are above U_c
        #    (3) have level_V - 1 <= level_W <= level_V
        # also, we make sure that no unit is put on stack more than once
        while stack != [] and len(seen_processes) < threshold:
            W = stack.pop()
            # this check is necessary since W might be of level == level_V (but cannot be of level < level_V - 1)
            if W.level == level_V - 1:
                seen_processes.add(W.creator_id)
            for W_parent in W.parents:
                if W_parent.level >= level_V - 1 and self.below(U_c, W_parent) and W_parent not in seen_units:
                    stack.append(W_parent)
                    seen_units.add(W_parent)

        memo[('proof', V_hash)] = len(seen_processes) >= threshold
        return memo[('proof', V_hash)]

    def _simple_coin(self, U, level):
        return (U.hash()[level%3])%2


    def super_majority(self, list_vals):
        '''
        Returns the value of supermajority of a list of bits.
        :param list list_vals: list of {0, 1}
        :returns: 0, 1 or -1, depending on whether there is supermajority (>=2/3 fraction) of 0s, 1s, or none of them
        '''
        treshold_majority = (2*self.n_processes + 2)//3
        if list_vals.count(1) >= treshold_majority:
            return 1
        if list_vals.count(0) >= treshold_majority:
            return 0

        return -1

    def default_vote(self, U, U_c):
        '''
        Default vote of U on popularity of U_c, as in the fast consensus algorithm.
        '''
        r = U.level - U_c.level - self.consensus_params['t_first_vote']
        assert r >= 1, "Default vote is asked on too low unit level."

        if r == 1:
            return 1

        if r == 2:
            return 0

        # something which depends upon U_c and U.level only: _simple_coin is good enough
        return self._simple_coin(U_c, U.level)


    def compute_vote(self, U, U_c):
        # determine the vote of unit U on popularity of U_c
        r = U.level - U_c.level - self.consensus_params['t_first_vote']
        assert r >= 0, "Vote is asked on too low unit level."
        U_c_hash, U_hash = U_c.hash(), U.hash()
        memo = self.timing_partial_results[U_c_hash]
        vote = memo.get(('vote', U_hash), None)

        if vote is not None:
            return vote

        if r == 0:
            # this should be in fact a "1" if any primt ancestor (at any level) of U proves popularity of U_c,
            # but it seems to be equivalent to the below
            vote = int(self.proves_popularity(U, U_c))
        else:
            votes_level_below = []
            for V in self.get_all_prime_units_by_level(U.level-1):
                vote_V = self.compute_vote(V, U_c)
                if vote_V == -1:
                    # NOTE: this should never happen at r=1, it will trigger an assert in default_vote if so
                    vote_V = self.default_vote(V, U_c)
                votes_level_below.append(vote_V)
            vote = self.super_majority(votes_level_below)

        memo[('vote', U_hash)] = vote
        return vote

    def decide_unit_is_popular(self, U_c):
        '''
        Decides popularity of U_c by going over all prime units that are high enough above U_c.
        '''
        logger = logging.getLogger(consts.LOGGER_NAME)
        U_c_hash = U_c.hash()

        if U_c_hash not in self.timing_partial_results:
            self.timing_partial_results[U_c_hash] = {}

        memo = self.timing_partial_results[U_c_hash]
        if 'decision' in memo.keys():
            return memo['decision']

        t = self.consensus_params['t_first_vote']

        for level in range(U_c.level + 2, U_c.level + t):
            for U in self.get_all_prime_units_by_level(level):
                if self.proves_popularity(U, U_c):
                    memo['decision'] = 1
                    process_id = (-1) if (self.process_id is None) else self.process_id
                    logger.info(f'decide_timing {process_id} | Timing unit for lvl {U_c.level} decided at lvl + {level - U_c.level}')
                    return 1



        for level in range(U_c.level + t + 1, self.level_reached + 1):
            for U in self.get_all_prime_units_by_level(level):
                decision = self.compute_vote(U, U_c)
                if decision == self.default_vote(U, U_c):
                    memo['decision'] = decision

                    if decision == 1:
                        process_id = (-1) if (self.process_id is None) else self.process_id
                        logger.info(f'decide_timing {process_id} | Timing unit for lvl {U_c.level} decided at lvl + {level - U_c.level}')

                    return decision
        return -1


    def decide_timing_on_level(self, level):
        '''
        Returns either a timing unit at this level or (-1) in case when no unit can be chosen yet.
        '''
        sigma = self.crp[level]

        for process_id in sigma:
            #In case there are multiple (more than one) units to consider (forking) we sort them by hashes (to break ties)
            prime_units_by_curr_process = sorted(self.prime_units_by_level[level][process_id], key = lambda U: U.hash())

            if len(prime_units_by_curr_process) == 0:
                # we have not seen any prime unit of this process at that level
                # there might still come one, so we need to wait, but no longer than till the level grows >= level + t
                # in which case a negative decision is guaranteed
                if self.level_reached >= level + self.consensus_params['t_first_vote']:
                    #we can safely skip this process as it will be decided 0 anyway
                    continue
                else:
                    #no decision can be made, need to wait
                    return -1

            for U_c in prime_units_by_curr_process:
                decision = self.decide_unit_is_popular(U_c)
                if decision == 1:
                    return U_c
                if decision == -1:
                    #we need to wait until the decision about this unit is made
                    return -1

        assert False, f"Something terrible happened: no timing unit was chosen at level {level}."


    def attempt_timing_decision(self):
        '''
        Tries to find timing units for levels which currently don't have one.
        :returns: List of timing units that have been established by this function call (in the order from lower to higher levels)
        '''
        timing_established = []
        for level in range(self.level_timing_established + 1, self.level_reached + 1):
            U_t = self.decide_timing_on_level(level)
            if U_t != -1:
                timing_established.append(U_t)
                self.timing_units.append(U_t)
                # need to clean up the memoized results about this level
                for U in self.get_all_prime_units_by_level(level):
                    self.timing_partial_results.pop(U.hash(), None)
            else:
                # don't need to consider next level if there is already no timing unit chosen for the current level
                break
        if timing_established:
            self.level_timing_established = timing_established[-1].level

        return timing_established
