'''This module implements a poset with a modified consensus rule (as compared to the whitepaper from Nov 2018).'''

import logging

from aleph.data_structures import Poset
import aleph.const as consts
from aleph.crypto.byte_utils import extract_bit




class FastPoset(Poset):
    '''
    An alternative instantiation of Poset -- with different consensus rules.
    '''
    def __init__(self, n_processes, process_id = None, crp = None, use_tcoin = None,
                compliance_rules = None, memo_height = 10):
        '''
        :param int n_processes: the committee size
        :param list compliance_rules: dictionary string -> bool
        '''
        self.n_processes = n_processes
        self.default_compliance_rules = {'forker_muting': True, 'parent_diversity': False, 'growth': False, 'expand_primes': True, 'threshold_coin': use_tcoin}
        self.compliance_rules = compliance_rules
        self.use_tcoin = use_tcoin if use_tcoin is not None else consts.USE_TCOIN
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
            for V in Vs:
                if self.below(V, U):
                    processes_below += 1
                    break

            # For efficiency:
            if 3*(processes_below + self.n_processes - 1 - process_id) < 2*self.n_processes:
                break

        # same as (...)>=2/3*(...) but avoids floating point division
        U.level = m+1 if 3*processes_below >= 2*self.n_processes else m
        return U.level

    def proves_popularity(self, V, U_c):
        '''
        Checks whether V proves that U_c is popular on V's level (i.e. everyone sees U on this level).
        More specifically we check whether there are >=2/3 N units W (created by distinct processes) such that
            (1) W <= V,
            (2) W has level <=level(V) - 2, or W is a prime unit at level(V)-1,
            (3) U_c <= W.
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
            memo[('proof', V_hash)] = False
            return False

        # implementation of a simple DFS from V down until we hit units that do not see U
        threshold = (2*self.n_processes + 2)//3
        seen_units = set([V])
        seen_processes = set()
        stack = [V]
        # the invariants here are that all elements W on stack:
        #    (1) are also in seen_units
        #    (2) are above U_c
        # also, we make sure that no unit is put on stack more than once
        while stack != [] and len(seen_processes) < threshold:
            W = stack.pop()
            if W.level <= level_V - 2 or (W.level == level_V - 1 and self.is_prime(W)):
                # if W is of level >= level_V - 1 and is not prime then it cannot be used for this proof
                seen_processes.add(W.creator_id)
            for W_parent in W.parents:
                if W_parent not in seen_units and self.below(U_c, W_parent):
                    stack.append(W_parent)
                    seen_units.add(W_parent)

        memo[('proof', V_hash)] = len(seen_processes) >= threshold
        return memo[('proof', V_hash)]


    def _simple_coin(self, U, level):
        # Needs to be a deterministic function of (U, level).
        # We choose it to be the l'th bit of U where l = level % (n_bits_in_U)
        l = level % (8 * len(U.hash()))
        return extract_bit(U.hash(), l)


    def default_vote(self, U, U_c):
        '''
        Default vote of U on popularity of U_c, as in the fast consensus algorithm.
        '''
        r = U.level - U_c.level - consts.VOTING_LEVEL
        assert r >= 1, "Default vote is asked on too low unit level."

        if r == 1:
            return 1

        if r == 2:
            return 0

        # something which depends upon U_c and U.level only: _simple_coin is good enough
        return self._simple_coin(U_c, U.level)


    def compute_vote(self, U, U_c):
        '''
        Determine the vote of unit U on popularity of U_c.
        If the first round of voting is at level L then:
            - at lvl L the vote is just whether U proves popularity of U_c (i.e. whether U_c <<< U)
            - at lvl (L+1) the vote is the supermajority of votes of prime ancestors (at level L)
            - at lvl (L+2) the vote is the supermajority of votes (replaced by default_vote if no supermajority) of prime ancestors (at level L+1)
            - etc.
        '''

        r = U.level - U_c.level - consts.VOTING_LEVEL
        assert r >= 0, "Vote is asked on too low unit level."
        U_c_hash, U_hash = U_c.hash(), U.hash()
        memo = self.timing_partial_results[U_c_hash]
        vote = memo.get(('vote', U_hash), None)

        if vote is not None:
            # this has been already computed and memoized in the past
            return vote

        if r == 0:
            # this should be in fact a "1" if any prime ancestor (at any level) of U proves popularity of U_c,
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
        Decides popularity of U_c (i.e. whether it should be a candidate for a timing unit).
        :returns: one of {-1,0,1}: the decision (0 or 1) in case it follows from our local view of the poset,
                  or -1 if the decision cannot be inferred yet
        '''
        logger = logging.getLogger(consts.LOGGER_NAME)
        U_c_hash = U_c.hash()

        if U_c_hash not in self.timing_partial_results:
            # set up memoization for this unit
            self.timing_partial_results[U_c_hash] = {}

        memo = self.timing_partial_results[U_c_hash]
        if 'decision' in memo.keys():
            return memo['decision']

        t = consts.VOTING_LEVEL
        t_p_d = consts.PI_DELTA_LEVEL

        # At levels +2, +3,..., +(t-1) it might be possible to prove that the consensus will be "1"
        # This is being tried in the loop below -- as Lemma 2.3.(1) in "Lewelewele" allows us to do:
        #   -- whenever there is unit U at one of this levels that proves popularity of U_c, we can conclude the decision is "1"
        for level in range(U_c.level + 2, U_c.level + t):
            for U in self.get_all_prime_units_by_level(level):
                if self.proves_popularity(U, U_c):
                    memo['decision'] = 1
                    process_id = (-1) if (self.process_id is None) else self.process_id
                    logger.info(f'decide_timing {process_id} | Timing unit for lvl {U_c.level} decided at lvl + {level - U_c.level}')
                    return 1


        # Attempt to make a decision using "The fast algorithm" from Def. 2.4 in "Lewelewele".
        for level in range(U_c.level + t + 1, min(U_c.level + t_p_d, self.level_reached + 1)):
            for U in self.get_all_prime_units_by_level(level):
                decision = self.compute_vote(U, U_c)
                # this is the crucial line: if the (supermajority) vote agrees with the default one -- we have reached consensus
                if decision == self.default_vote(U, U_c):
                    memo['decision'] = decision

                    if decision == 1:
                        process_id = (-1) if (self.process_id is None) else self.process_id
                        logger.info(f'decide_timing {process_id} | Timing unit for lvl {U_c.level} decided at lvl + {level - U_c.level}')

                    return decision

        # Switch to the pi-delta algorithm if consensus could not be reached using the "fast algorithm".
        # It guarantees termination after a finite number of levels with probability 1.
        # Note that this piece of code will only execute if there is still no decision on U_c and level_reached is >= U_c.level + t_p_d,
        #   which we consider rather unlikely to happen since under normal circumstances (no malicious adversary) the fast algorithm
        #   will likely decide at level <= +5. The default value of t_p_d is 15, thus after reaching level +6 and assuming that default_vote
        #   is a random function of level, the probability of reaching level 15 is <= 2^{-10} <= 10^{-3}.
        for level in range(U_c.level + t_p_d + 1, self.level_reached + 1, 2):
            # Note that we always jump by two levels because of the specifics of this consensus protocol.
            # Note that we start at U_c.level + t_p_d + 1 because U_c.level + t_p_d we consider as an "odd" round
            #    and only the next one is the first "even" round where delta is supposed to be computed.
            for U in self.get_all_prime_units_by_level(level):
                decision = self.compute_delta(U_c, U)
                if decision != -1:
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

        if self.level_reached < level + consts.VOTING_LEVEL:
            # We cannot decide on a timing unit yet since there might be units that we don't see.
            # After reaching lvl level + consts.VOTING_LEVEL, if we do not see some unit it will necessarily be decided 0.
            return -1

        sigma = self.crp[level]

        for process_id in sigma:
            #In case there are multiple (more than one) units to consider (forking) we sort them by hashes (to break ties)
            prime_units_by_curr_process = sorted(self.prime_units_by_level[level][process_id], key = lambda U: U.hash())

            for U_c in prime_units_by_curr_process:
                decision = self.decide_unit_is_popular(U_c)
                if decision == 1:
                    return U_c
                if decision == -1:
                    #we need to wait until the decision about this unit is made
                    return -1

        assert False, f"Something terrible happened: no timing unit was chosen at level {level}."


    def exists_tc(self, list_vals, U_c, tossing_unit):
        if 1 in list_vals:
            return 1
        if 0 in list_vals:
            return 0
        return self.toss_coin(U_c, tossing_unit)


    def super_majority(self, list_vals):
        treshold_majority = (2*self.n_processes + 2)//3
        if list_vals.count(1) >= treshold_majority:
            return 1
        if list_vals.count(0) >= treshold_majority:
            return 0

        return -1


    def compute_pi(self, U_c, U):
        '''
        Computes the value of the Pi function from the paper. The value -1 is equivalent to bottom (undefined).
        '''
        # "r" is the number of level of the pi_delta protocol.
        # Note that level U_c.level + consts.consts.PI_DELTA_LEVEL has number 1 because we want it to execute an "odd" round
        r = U.level - (U_c.level + consts.consts.PI_DELTA_LEVEL) + 1
        assert r >= 1, "The pi_delta protocol is attempted on a too low of a level."
        U_c_hash = U_c.hash()
        U_hash = U.hash()
        memo = self.timing_partial_results[U_c_hash]

        pi_value = memo.get(('pi', U_hash), None)
        if pi_value is not None:
            return pi_value

        r_value = self.r_function(U_c, U)

        votes_level_below = []

        for V in self.get_all_prime_units_by_level(U.level-1):
            if self.below(V, U):
                if r == 1:
                    # we use the votes of the last round of the "fast algorithm"
                    vote_V = self.compute_vote(V, U_c)
                    vote = vote_V if vote_V != -1 else self.default_vote(V, U_c)
                    votes_level_below.append(vote)
                else:
                    # we use the pi-values of the last round
                    votes_level_below.append(self.compute_pi(U_c, V))

        if r % 2 == 0:
            # the "exists" round
            pi_value = self.exists_tc(votes_level_below, U_c, U)
        elif r % 2 == 1:
            # the "super-majority" round
            pi_value = self.super_majority(votes_level_below)

        memo[('pi', U_hash)] = pi_value
        return pi_value


    def compute_delta(self, U_c, U):
        '''
        Computes the value of the Delta function from the paper. The value -1 is equivalent to bottom (undefined).
        '''
        U_c_hash = U_c.hash()
        U_hash = U.hash()
        memo = self.timing_partial_results[U_c_hash]

        delta_value = memo.get(('delta', U_hash), None)
        if delta_value is not None:
            return delta_value

        # "r" is the number of level of the pi_delta protocol (see also the comment in compute_pi)
        r = U.level - (U_c.level + consts.consts.PI_DELTA_LEVEL) + 1

        assert r % 2 == 0, "Delta is attempted to be evaluated at an odd level."

        pi_values_level_below = []
        for V in self.get_all_prime_units_by_level(U.level-1):
            if self.high_below(V, U):
                pi_values_level_below.append(self.compute_pi(U_c, V))

        delta_value = self.super_majority(pi_values_level_below)
        memo[('delta', U_hash)] = delta_value
        return delta_value