'''This module implements a poset - a core data structure.'''

from itertools import product
from functools import reduce
import random
import logging

from aleph.crypto.signatures.threshold_signatures import generate_keys
from aleph.crypto.threshold_coin import ThresholdCoin

from aleph.data_structures.unit import Unit
from aleph.crypto import xor
from aleph.config import *



class Poset:
    '''This class is the core data structure of the Aleph protocol.'''


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
        self.min_non_validated = [[] for _ in range(n_processes)]
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


#===============================================================================================================================
# UNITS
#===============================================================================================================================

    def prepare_unit(self, U):
        '''
        Sets basic fields of U; should be called prior to check_compliance and add_unit methods.
        This method does the following:
            0. set U's self_predecessor and height
            1. set floor field
            2. set U's level
            3. if it is prime and of level >=4, add coin shares to it
        :param unit U: unit which fields are about to be set
        '''

        # 0. set U's self_predecessor and height
        self.set_self_predecessor_and_height(U)

        # 1. set floor field
        U.floor = [[] for _ in range(self.n_processes)]
        self.update_floor(U)

        # 2. set U's level
        U.level = self.level(U)

        # 3. if it is prime of level >= ADD_SHARES, add coin shares to it
        #if add_tcoin_shares and self.is_prime(U) and U.level >= ADD_SHARES:
        #    self.add_coin_shares(U)


    def add_unit(self, U, newly_validated = None):
        '''
        Add a unit compliant with the rules, what was checked by check_compliance.
        This method does the following:
            0. add the unit U to the poset
            1. if it is a dealing unit, add it to self.dealing_units
            2. update the lists of maximal elements in the poset.
            3. update forking_height
            4. if U is prime, add it to prime_units_by_level
            5. set ceil attribute of U and update ceil of predecessors of U
            6. validate units using U if possible and updates the border between validated and non-validated units
            7. if required, adds U to memoized_units
        :param unit U: unit to be added to the poset
        :returns: It does not return anything explicitly but modifies the newly_validated list: adds the units validated by U
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


        # 2. updates the lists of maximal elements in the poset and
        if len(U.parents) == 0:
            assert self.max_units_per_process[U.creator_id] == [], "A second dealing unit is attempted to be added to the poset"
            self.max_units_per_process[U.creator_id] = [U]
        else:
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

        # 5. set ceil attribute of U and update ceil of predecessors of U
        U.ceil = [[] for _ in range(self.n_processes)]
        U.ceil[U.creator_id] = [U]
        for parent in U.parents:
            self.update_ceil(U, parent)

        # 6. validate units and update the "border" of non_validated units
        if newly_validated is not None:
            newly_validated.extend(self.validate_using_new_unit(U))

        # the below might look over-complicated -- this is because of potentials forks of U's creator
        # ignoring forks this simplifies to: if min_non_validated[U.creator_id] == [] then add U to it
        if not any(self.below_within_process(V, U) for V in  self.min_non_validated[U.creator_id]):
            self.min_non_validated[U.creator_id].append(U)

        # 7. Update memoized_units
        if U.height % self.memo_height == 0:
            n_units_memoized = len(self.memoized_units[U.creator_id])
            U_no = U.height//self.memo_height
            if n_units_memoized >= U_no + 1:
                #this means that U.creator_id is forking and there is already a unit added on this height
                pass
            else:
                assert n_units_memoized == U_no, f"The number of units memoized is {n_units_memoized} while it should be {U_no}."
                self.memoized_units[U.creator_id].append(U)


    def create_unit(self, creator_id, txs, strategy = "link_self_predecessor", num_parents = 2, force_parents = None):
        '''
        Creates a new unit and stores txs in it. Correctness of the txs is checked by a thread listening for new transactions.
        :param list txs: list of correct transactions
        :param string strategy: strategy for parent selection, one of:
        - "link_self_predecessor"
        - "link_above_self_predecessor"
        :param int num_parents: number of distinct parents
        :param list force_parents: (ONLY FOR DEBUGGING/TESTING) parents (units) for the created unit
        :returns: the new-created unit, or None if it is not possible to create a compliant unit
        '''

        # NOTE: perhaps we (as an honest process) should always try (if possible)
        # NOTE: to create a unit that gives evidence of another process forking
        logger = logging.getLogger(LOGGER_NAME)
        U = Unit(creator_id, [], txs)
        logger.info(f"create: {creator_id} attempting to create a unit.")
        if len(self.max_units_per_process[creator_id]) == 0:
            # this is going to be our dealing unit
            if force_parents is not None:
                assert force_parents == [], "A dealing unit should be created first."
            if self.use_tcoin:
                self.add_tcoin_to_dealing_unit(U)
            logger.info(f"create: {creator_id} created its dealing unit.")
            return U


        assert len(self.max_units_per_process[creator_id]) == 1, "It appears we have created a fork."

        if force_parents is not None:
            assert len(force_parents) == num_parents, "Incorrect number of parents chosen."

        if force_parents is None:
            U_max = self.max_units_per_process[creator_id][0]

            single_tip_processes = set(pid for pid in range(self.n_processes)
                                    if len(self.max_units_per_process[pid]) == 1)

            growth_restricted = set(pid for pid in single_tip_processes if self.below(self.max_units_per_process[pid][0], U_max))

            recent_parents = set()

            W = U_max

            while True:
                # W is our dealing unit -> STOP
                if len(W.parents) == 0:
                    break

                parents = [V.creator_id for V in W.parents if V.creator_id != creator_id]

                # recent_parents.update(parents)
                # ceil(n_processes/3)

                treshold = (self.n_processes+2)//3
                if len(recent_parents.union(parents)) >= treshold:
                    break

                recent_parents = recent_parents.union(parents)
                W = W.self_predecessor

            legit_parents = [pid for pid in single_tip_processes if not (pid in growth_restricted or pid in recent_parents)]
            legit_parents = list(set(legit_parents + [creator_id]))

            if len(legit_parents) < num_parents:
                return None

            legit_below = [pid for pid in legit_parents if self.below(U_max, self.max_units_per_process[pid][0])]


            if strategy == "link_self_predecessor":
                first_parent = creator_id
            elif strategy == "link_above_self_predecessor":
                first_parent = random.choice(legit_below)
            else:
                raise NotImplementedError("Strategy %s not implemented" % strategy)

            random.shuffle(legit_parents)
            parent_processes = legit_parents[:num_parents]
            if first_parent in parent_processes:
                parent_processes.remove(first_parent)
            else:
                parent_processes.pop()
            parent_processes = [first_parent] + parent_processes

            U.parents = [self.max_units_per_process[pid][0] for pid in parent_processes]
        else:
            # forced_parents is set
            assert all(V.hash() in self.units for V in forced_parents)
            # compliance might still fail here -- but it will be detected later
            # forced_parents should be used for debugging and testing purposes only
            U.parents = forced_parents

        if self.use_tcoin:
            self.prepare_unit(U)
            if self.is_prime(U) and U.level >= ADD_SHARES:

                self.add_coin_shares(U)


        return U


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

        # need to count all processes that produced a unit V of level m such that U'<<U
        # we can limit ourselves to prime units V
        processes_high_below = 0

        for process_id in range(self.n_processes):
            Vs = self.prime_units_by_level[m][process_id]
            if any(self.high_below(V, U) for V in Vs):
                processes_high_below += 1

            # For efficiency: break the loop if there is no way to collect supermajority
            if 3*(processes_high_below + self.n_processes - 1 - process_id) < 2*self.n_processes:
                break


        # same as (...)>=2/3*(...) but avoids floating point division
        U.level = m+1 if 3*processes_high_below >= 2*self.n_processes else m
        return U.level


    def is_prime(self, U):
        '''
        Check if the unit is prime.
        :param unit U: the unit to be checked for being prime
        '''
        # U is prime iff it's a bottom unit or its self_predecessor level is strictly smaller
        return len(U.parents) == 0 or self.level(U) > self.level(U.self_predecessor)


    def determine_coin_shares(self, U):
        '''
        Determines which coin shares should be added to the prime unit U as described in arxiv whitepaper.
        :param unit U: prime unit to which coin shares should be added
        :returns: list of pairs of indices such that for (i,j) in the list the coin share TC^j_i(L(U)) should be added to U
        '''

        # NOTE there is a problem with lemma 3.16 (there could be no such W), i.e. there could be not enough coin shares to toss a coin.
        # as a solution on level +4: if there is enough shares it succeeds, otherwise just toss a hash of U,
        # and on level +6 we know there are enough shares

        # don't add coin shares for prime units of level lower than 6
        # this is due to the fact that we want to build transversal for a family
        # of sets of dealing units in lower cones of prime units of level 3
        if U.level < ADD_SHARES:
            return []

        # the to-be-constructed list of pairs of indices such that for (i,j) in the list the coin share TC^j_i(L(U)) should be added to U
        indices = []

        # don't add coin shares of dealers that are proved by U to be forkers or U does not see a dealing unit
        skip_dealer_ind = set(dealer_id for dealer_id in range(self.n_processes) if self.has_forking_evidence(U, dealer_id) or
                                                                                    self.index_dealing_unit_below(dealer_id, U) is None)

        share_id = U.creator_id

        # starting from level 3 there is negligible probability that the transversal will have more than 1 element
        # as we start adding coin shares to units of level 6, everything is just fine
        level = 3

        # construct the list of all prime units below U at level 3
        # it can be proved that these are enough instead of *all* prime units at levels 3<= ... <= U.level-3
        prime_below_U = [V for Vs in self.prime_units_by_level[level] for V in Vs if self.below(V, U)]


        sigma = self.crp[U.level]

        for i, dealer_id in enumerate(sigma):
            # don't add shares for forking dealers
            if dealer_id in skip_dealer_ind:
                continue

            indices.append((share_id, dealer_id))

            # during construction of skip_dealer_ind we ruled out the possibility that the below returns None
            ind_dU = self.index_dealing_unit_below(dealer_id, U)
            dU = self.dealing_units[dealer_id][ind_dU]

            # filter out all units that have dU<=V
            prime_below_U = [V for V in prime_below_U if not self.below(dU, V)]

            # have we added all necessary shares?
            if prime_below_U == []:
                break
        return indices


    def add_coin_shares(self, U):
        '''
        Adds coin shares to the prime unit U as described in arxiv whitepaper.
        :param unit U: prime unit to which coin shares are added
        '''

        coin_shares = []
        indices = self.determine_coin_shares(U)
        if U.level >= ADD_SHARES:
            assert indices != [] and indices is not None

        for _, dealer_id in indices:
            ind = self.index_dealing_unit_below(dealer_id, U)

            # assert self.threshold_coins[dealer_id][ind].process_id == U.creator_id
            # we can take threshold coin of index ind as it is included in the unique dealing unit by dealer_id below U
            coin_shares.append(self.threshold_coins[dealer_id][ind].create_coin_share(U.level))

        U.coin_shares = coin_shares

    def add_tcoin_to_dealing_unit(self, U):
        '''
        Adds threshold coins for all processes to the unit U. U is supposed to be the dealing unit for this to make sense.
        NOTE: to not create a new field in the Unit class the coin_shares field is reused to hold treshold coins in dealing units.
        (There will be no coin shares included at level 0 anyway.)
        '''
        U.coin_shares = []
        vk, sks = generate_keys(self.n_processes, self.n_processes//3+1)
        for process_id in range(self.n_processes):
            # create and append the threshold coin black-box for committee member no process_id
            threshold_coin = ThresholdCoin(self.process_id, process_id, self.n_processes, self.n_processes//3+1, sks[process_id], vk)
            U.coin_shares.append(threshold_coin)


    def get_all_prime_units_by_level(self, level):
        '''
        Returns the set of all prime units at a given level.
        :param int level: the requested level of units
        '''
        if level not in self.prime_units_by_level.keys():
            return []
        return [V for Vs in self.prime_units_by_level[level] for V in Vs]


    def get_prime_units_by_level_per_process(self, level):
        '''
        Returns the set of all prime units at a given level.
        :param int level: the requested level of units
        '''
        # TODO: this is a naive implementation
        # TODO: make sure that at creation of a prime unit it is added to the dict self.prime_units_by_level
        assert level in self.prime_units_by_level.keys()
        return self.prime_units_by_level[level]


    def set_self_predecessor_and_height(self, U):
        '''
        Checks if the unit U has a uniquely-, well defined self predecessor.
        In other words, there is at least one unit below U, created by U.creator_id and
        U respects the anti-diamond policy, i.e. the following situation is not allowed:
            - The creator of U is the process j
            AND
            - The parents of U (combined) have evidence that j is forking
        If the check is succesful, the method sets U.self_predecessor and U.height
        :param unit U: the unit whose self_predecessor is being checked
        :returns: Boolean value, True if U has a well-defined self_predecessor
        '''
        if len(U.parents) == 0:
            U.self_predecessor = None
            U.height = 0
            return True
        else:
            combined_floors = self.combine_floors_per_process(U.parents, U.creator_id)
            #assert (len(combined_floors) >= 1), "Unit U has no candidates for predecessors."
            #assert (len(combined_floors) <= 1), "Unit U has more than one candidate for predecessor."
            if len(combined_floors) == 1:
                U.self_predecessor = combined_floors[0]
                U.height = U.self_predecessor.height + 1
                return True
            else:
                return False


    def unit_by_hash(self, unit_hash):
        '''
        Returns a unit in the poset given by its hash, or None if not present.
        '''

        return self.units.get(unit_hash, None)


    def units_by_height_interval(self, creator_id, min_height, max_height):
        '''
        Simple function for testing listener.
        '''
        if min_height > max_height or min_height > self.max_units_per_process[creator_id][0].height:
            return []

        units = []
        U = self.max_units_per_process[creator_id][0]
        while U is not None and U.height >= min_height:
            if U.height<=max_height:
                units.append(U)
            U = U.self_predecessor

        return reversed(units)


    def get_max_heights_hashes(self):
        '''
        Simple function for testing listener.
        Assumes no forks exist.
        '''
        heights, hashes = [], []

        for U_l in self.max_units_per_process:
            if len(U_l) > 0:
                U = U_l[0]
                heights.append(U.height)
                hashes.append(U.hash())
            else:
                heights.append(-1)
                hashes.append(None)

        return heights, hashes


    def get_diff(self, process_id, current, prev):
        '''
        Outputs the set of all units U such that U lies strictly above some unit in prev and below some unit in current.
        The output is in topological order.
        Both current and prev are assumed to be collections of maximal units within process_id.
        Also the output are only units in process_id
        '''
        diff_hashes = set()
        curr_hashes = set(U.hash() for U in current)
        prev_hashes = set(U.hash() for U in prev)
        while len(curr_hashes) > 0:
            U_hash = curr_hashes.pop()
            U = self.units[U_hash]
            if U_hash not in prev_hashes and U_hash not in diff_hashes:
                diff_hashes.add(U_hash)
                if U.self_predecessor is not None:
                    curr_hashes.add(U.self_predecessor.hash())

        return [self.units[U_hash] for U_hash in diff_hashes]


#===============================================================================================================================
# COMPLIANCE
#===============================================================================================================================


    def should_check_rule(self, rule):
        '''
        Check whether the rule (a string) "forker_muting", "parent_diversity", etc. should be checked in the check_compliance function.
        Based on the combination of default values and the compliance_rules dictionary provided as a parameter to the constructor.
        :returns: True or False
        '''
        assert rule in self.default_compliance_rules

        if self.compliance_rules is None or rule not in self.compliance_rules:
            return self.default_compliance_rules[rule]

        return self.compliance_rules[rule]


    def check_compliance(self, U):
        '''
        Assumes that prepare_unit(U) has been already called.
        Checks if the unit U is correct and follows the rules of creating units, i.e.:
            1. Parents of U are correct (exist in the poset, etc.)
            2. Has correct signature.
            3. U has a well-defined self_predecessor
            4. Satisfies forker-muting policy.
            5. Satisfies parent diversity rule.
            6. Check "growth" rule.
            7. The coinshares are OK, i.e., U contains exactly the coinshares it is supposed to contain.
        :param unit U: unit whose compliance is being tested
        '''
        # TODO: it is highly desirable that there are no duplicate transactions in U (i.e. literally copies)

        # 1. Parents of U are correct.
        if not self.check_parent_correctness(U):
            return False

        # 2. Has correct signature.
        if not self.check_signature_correct(U):
            return False

        # This is a dealing unit, and its signature is correct --> it is compliant
        if len(U.parents) == 0:
            self.set_self_predecessor_and_height(U)
            return True

        # 3. U has a well-defined self_predecessor

        if not self.set_self_predecessor_and_height(U):
            return False

        # At this point we know that U has a well-defined self_predecessor
        # and the corresponding field U.self_predecessor is set


        # 4. Satisfies forker-muting policy.
        if self.should_check_rule('forker_muting'):
            if not self.check_forker_muting(U):
                return False

        # 5. Satisfies parent diversity rule.
        if self.should_check_rule('parent_diversity'):
            if not self.check_parent_diversity(U):
                return False

        # 6. Check "growth" rule.
        if self.should_check_rule('growth'):
            if not self.check_growth(U):
                return False

        # 7. Coinshares are OK.
        if self.should_check_rule('threshold_coin'):
            if self.is_prime(U) and not self.check_coin_shares(U):
                return False

        return True


    def check_growth(self, U):
        '''
        Checks if the unit U, created by process j, respects the "growth" rule.
        No parent of U can be below the self predecessor of U.
        :param unit U: unit that is tested against the grow rule
        :returns: Boolean value, True if U respects the rule, False otherwise.
        '''
        # U.self_predecessor should be correctly set when invoking this method

        if len(U.parents) == 0:
            return True

        assert (U.self_predecessor is not None), "The self_predecessor field has not been filled for U"

        for V in U.parents:
            if (V is not U.self_predecessor) and self.below(V, U.self_predecessor):
                return False
        return True


    def check_forker_muting(self, U):
        '''
        Checks if the unit U respects the forker-muting policy, i.e.:
        The following situation is not allowed:
            - There exists a process j, s.t. one of U's parents was created by j
            AND
            - U has as one of the parents a unit that has evidence that j is forking.
        :param unit U: unit that is checked for respecting anti-forking policy
        :returns: Boolean value, True if U respects the forker-muting policy, False otherwise.
        '''

        if len(U.parents) == 0:
            return True

        parent_processes = set([V.creator_id for V in U.parents])
        for V, proc in product(U.parents, parent_processes):
            if self.has_forking_evidence(V, proc):
                return False

        return True


    def check_signature_correct(self, U):
        '''
        Checks if the signature of a unit U is correct.
        :param unit U: unit whose signature is checked
        '''

        #TODO: temporarily returns just True
        #TODO: need to complete this code once the signature method is decided on

        return True


    def check_parent_correctness(self, U):
        '''
        Checks whether U has correct parents:
        1. Parents of U exist in the poset
        2. If U has >=2 parents then all parents are created by pairwise different processes.
        :param unit U: unit whose parents are being checked
        '''
        # 1. Parents of U exist in the poset
        for V in U.parents:
            if V.hash() not in self.units.keys():
                return False

        # 2. If U has parents created by pairwise different processes.
        if len(U.parents) >= 2:
            parent_processes = set([V.creator_id for V in U.parents])
            if len(parent_processes) < len(U.parents):
                return False

        return True


    def check_parent_diversity(self, U):
        '''
        Checks if unit U satisfies the parrent diversity rule:
        Let j be the creator process of unit U,
        if U wants to use a process i as a parent for U and:
        - previously it created a unit U_1 at height h_1 with parent i,
        - unit U has height h_2 with h_1<h_2.
        then consider the set P of all processes that were used as parents
        of nodes created by j at height h, s.t. h_1 <= h < h_2,
        (i can be used as a parent for U) iff (|P|>=n_processes/3)
        Note that j is not counted in P.
        :param unit U: unit whose parent diversity is being tested
        '''

        # Special case: U is a dealing unit
        if len(U.parents) == 0:
            return True

        # TODO: make sure U.self_predecessor is correctly set when invoking this method
        assert (U.self_predecessor is not None), "The self_predecessor field has not been filled for U"

        proposed_parent_processes = [V.creator_id for V in U.parents]
        # in case U's creator is among parent processes we can ignore it
        if U.creator_id in proposed_parent_processes:
            proposed_parent_processes.remove(U.creator_id)
        # bitmap for checking whether a given process was among parents
        was_parent_process = [False for _ in range(self.n_processes)]
        # counter for maintaining sum(was_parent_process)
        n_parent_processes = 0

        W = U.self_predecessor
        # traverse the poset down from U, through self_predecessor
        while True:
            # W is a bottom unit -> STOP
            if len(W.parents) == 0:
                break
            # flag for whether at the current level there is any occurence of a parent process proposed by U
            proposed_parent_process_occurence = False

            for V in W.parents:
                if V.creator_id != U.creator_id:
                    if V.creator_id in proposed_parent_processes:
                        # V's creator is among proposed parent processes
                        proposed_parent_process_occurence = True

                    if not was_parent_process[V.creator_id]:
                        was_parent_process[V.creator_id] = True
                        n_parent_processes += 1

            if n_parent_processes*3 >= self.n_processes:
                break

            if proposed_parent_process_occurence:
                # a proposed parent process repeated too early!
                return False

            W = W.self_predecessor

        return True


    def check_coin_shares(self, U):
        '''
        Checks if coin shares stored in U are OK.
        :param unit U: unit which coin shares are checked
        :returns: True if everything works and False otherwise
        '''

        #TODO check if shares are valid, i.e. if they can be combined
        indices = self.determine_coin_shares(U)
        if U.coin_shares is None:
            if indices:
                return False
            else:
                return True
        if len(indices) != len(U.coin_shares):
            return False

        for (share_id, dealer_id), coin_share in zip(indices, U.coin_shares):
            # there should be exactly one threshold coin below U
            ind = self.index_dealing_unit_below(dealer_id, U)
            if not self.threshold_coins[dealer_id][ind].verify_coin_share(coin_share, share_id, U.level):
                return False

        return True


#===============================================================================================================================
# FLOOR AND CEIL
#===============================================================================================================================


    def update_floor(self, U):
        '''
        Updates floor of the unit U by merging and taking maximums of floors of parents.
        '''
        U.floor[U.creator_id] = [U]
        if U.parents:
            for process_id in range(self.n_processes):
                if process_id != U.creator_id:
                    U.floor[process_id] = self.combine_floors_per_process(U.parents, process_id)


    def update_ceil(self, U, V):
        '''
        Adds U to the ceil of V if U is not comparable with any unit already present in ceil V.
        If such an addition happens, ceil is updated recursively in the lower cone of V.
        '''
        # TODO: at some point we should change it to a version with an explicit stack
        # TODO: Python has some strange recursion depth limits

        # if U is above any of V.ceil[i] then no update is needed in V nor its lower cone
        for W in V.ceil[U.creator_id]:
            if self.below_within_process(W, U):
                return
        # U is not above any of V.ceil[i], needs to be added and propagated recursively
        V.ceil[U.creator_id].append(U)
        for parent in V.parents:
            self.update_ceil(U, parent)


    def combine_floors_per_process(self, units_list, process_id):
        '''
        Combines U.floor[process_id] for all units U in units_list.
        The result is the set of maximal elements of the union of these lists.
        :param list units_list: list of units to be considered
        :param int process_id: identification number of a process
        :returns: list U that contains maximal elements of the union of floors of units_list w.r.t. process_id
        '''
        assert len(units_list) > 0, "combine_floors_per_process was called on an empty unit list"

        # initialize forks with the longest floor from units_list
        lengths = [len(U.floor[process_id]) for U in units_list]
        index = lengths.index(max(lengths))
        forks = units_list[index].floor[process_id][:]

        #gather all other floor members in one list
        candidates = [V for i, U in enumerate(units_list) if i != index for V in U.floor[process_id]]

        for U in candidates:
            # This flag checks if there is W comparable with U. If not then we add U to forks
            found_comparable, replace_index = False, None
            for k, W in enumerate(forks):
                if U.height > W.height and self.above_within_process(U, W):
                    found_comparable = True
                    replace_index = k
                    break
                if U.height <= W.height and self.below_within_process(U, W):
                    found_comparable = True
                    break

            if not found_comparable:
                forks.append(U)

            if replace_index is not None:
                forks[replace_index] = U

        return forks


    def has_forking_evidence(self, U, process_id):
        '''
        Checks if U has in its lower cone an evidence that process_id is forking.
        :param Unit U: unit to be checked for evidence of process_id forking
        :param int process_id: identification number of process to be verified
        :returns: True if forking evidence is present, False otherwise
        '''
        return len(U.floor[process_id]) > 1


    def index_dealing_unit_below(self, dealer_id, U):
        '''
        Returns an index of dealing unit created by dealer_id that is below U or None otherwise.
        '''

        n_dunits_below, ind_dU_below = 0, None
        for ind, dU in enumerate(self.dealing_units[dealer_id]):
            if self.below(dU, U):
                n_dunits_below += 1
                ind_dU_below = ind

            if n_dunits_below > 1:
                return None

        return ind_dU_below


#===============================================================================================================================
# RELATIONS
#===============================================================================================================================


    def below_within_process(self, U, V):
        '''
        Checks if there exists a path (possibly U = V) from U to V going only through units created by their creator process.
        Assumes that U.creator_id = V.creator_id = process_id
        :param unit U: first unit to be tested
        :param unit V: second unit to be tested
        '''
        assert (U.creator_id == V.creator_id and U.creator_id is not None) , "expected two units created by the same process"
        if U.height > V.height:
            return False
        process_id = U.creator_id
        # if process_id is non-forking or at least U is below the process_id's forking level then clearly U has a path to V
        if U.height < self.forking_height[process_id]:
            return True

        # at this point we know that this is a forking situation: we need go down the tree from V until we reach U's height
        # this will not take much time as process_id is banned for forking right after it is detected

        W = V
        while W.height > U.height:
            W = W.self_predecessor

        # TODO: make sure the below line does what it should
        return (W is U)


    def strictly_below_within_process(self, U, V):
        '''
        Checks if there exists a path from U to V going only through units created by their creator process.
        It is not allowed that U == V.
        Assumes that U.creator_id = V.creator_id = process_id
        :param unit U: first unit to be tested
        :param unit V: second unit to be tested
        '''
        return (U is not V) and self.below_within_process(U,V)


    def above_within_process(self, U, V):
        '''
        Checks if there exists a path (possibly U = V) from V to U going only through units created by their creator process.
        Assumes that U.creator_id = V.creator_id = process_id
        :param unit U: first unit to be tested
        :param unit V: second unit to be tested
        '''
        return self.below_within_process(V, U)


    def below(self, U, V):
        '''
        Checks if U <= V.
        :param unit U: first unit to be tested
        :param unit V: second unit to be tested
        '''
        for W in V.floor[U.creator_id]:
            if self.below_within_process(U, W):
                return True
        return False


    def above(self, U, V):
        '''
        Checks if U >= V.
        :param unit U: first unit to be tested
        :param unit V: second unit to be tested
        '''
        return self.below(V, U)


    def high_below(self, U, V):
        '''
        Checks if U << V.
        :param unit U: first unit to be tested
        :param unit V: second unit to be tested
        '''
        processes_in_support = 0
        for process_id in range(self.n_processes):
            #if process_id == U.creator_id or process_id == V.creator_id:
            #    processes_in_support += 1
            #    continue

            in_support = False
            # Because process_id could be potentially forking, we need to check
            # if there exist U_ceil in U.ceil[process_id] and V_floor in V.floor[process_id]
            # such that U_ceil <= V_floor.
            # In the case when process_id is non-forking, U' and V' are unique and the loops below are trivial.
            for U_ceil in U.ceil[process_id]:
                # for efficiency: if answer is true already, terminate loop
                if in_support:
                    break
                for V_floor in V.floor[process_id]:
                    if self.below_within_process(U_ceil, V_floor):
                        in_support = True
                        break

            if in_support:
                processes_in_support += 1

            # For efficiency: break the loop if there is no way to collect supermajority
            if 3*(processes_in_support + (self.n_processes-1-process_id)) < 2*self.n_processes:
                break

            # This might be a little bit faster (and more Pythonic ;))
            #if any([self.below_within_process(U_ceil, V_floor) for U_ceil in U.ceil[process_id] for V_floor in V.floor[process_id]]):
            #    processes_in_support += 1

        # same as processes_in_support>=2/3 n_procesees but avoids floating point division
        return 3*processes_in_support >= 2*self.n_processes


    def high_above(self, U, V):
        '''
        Checks if U >> V.
        :param unit U: first unit to be tested
        :param unit V: second unit to be tested
        '''
        return self.high_below(V, U)


#===============================================================================================================================
# PI AND DELTA FUNCTIONS
#===============================================================================================================================


    def r_function(self, U_c, U):
        '''
        The R function from the paper
        :returns: a value in {-1,0,1}, -1 is equivalent to bottom (undefined).
        '''
        if U.level <= U_c.level +1:
            return -1
        return (U.level - U_c.level) % 2


    def first_available_index(self, V, level):
        permutation = self.crp[level]

        for dealer_id in permutation:
            if self.has_forking_evidence(V, dealer_id):
                continue
            if any(self.below(U,V) for U in self.dealing_units[dealer_id]):
                return dealer_id

        #This is clearly a problem... Should not happen
        assert False, "No index available for first_available_index."
        return None


    def _simple_coin(self, U, level):
        return (U.hash()[level%3])%2


    def toss_coin(self, U_c, tossing_unit):
        # in this implementation we don't use info about who dealt a threshold coin that was
        # used to generate a share, i.e. Unit.coin_shares is a list and does not contain that
        # info. This way we save space, but we need to figure this info out when we toss a coin.
        # Alternative approach would be to have Unit.coin_shares as a dict of pairs
        # (dealer_id, coin_share). This would require more space, but ease implementation and speed
        # tossing a coin. We believe that tossing coin is rare, hence current implementation is chosen
        logger = logging.getLogger(LOGGER_NAME)
        logger.info(f'Tossing coin at level {tossing_unit.level} for a unit at level {U_c.level}.')
        print('coin_toss!!!11')
        #exit(0)

        if self.use_tcoin == False:
            return self._simple_coin(U_c, tossing_unit.level-1)

        level = tossing_unit.level-1
        fai = self.first_available_index(U_c, level)

        # we use simple_coin if tossing_unit already knows that fai is a forker
        if self.has_forking_evidence(tossing_unit, fai):
            return self._simple_coin(U_c, level)

        # in case fai has forked, there might be multiple his dealing units -- here we pick the (unique) one below U_c
        ind_dealer = self.index_dealing_unit_below(fai, U_c)
        # at this point (after fai was determined) it must be the case that there is exactly one dealing unit by fai below U_c
        assert ind_dealer is not None

        dU = self.dealing_units[fai][ind_dealer]


        coin_shares = {}

        sigma = self.crp[level]

        # run through all prime ancestors of the tossing_unit
        for V in self.get_all_prime_units_by_level(level):
            # we gathered enough coin shares -- ceil(n_processes/3)
            if len(coin_shares) == self.n_processes//3 + 1:
                break

            # can use only shares from units visible from the tossing unit (so that every process arrives at the same result)
            # note that being high_below here is not necessary
            if not self.below(V, tossing_unit):
                continue

            # check if V is a fork and we have added coin share corresponding to its creator
            # Note that at this point we know that fai has not forked its dealing Unit (at least not below tossing_unit)
            # and the shares in V are validated hence even if V.creator_id forked, the share should be identical
            if V.creator_id in coin_shares:
                continue

            # TODO try to optimize this part
            indices = self.determine_coin_shares(V)
            for cs_ind, (share_id, dealer_id) in enumerate(indices):
                assert share_id == V.creator_id
                if dealer_id == fai:
                    coin_shares[V.creator_id] = V.coin_shares[cs_ind]
                    break



        # we have enough valid coin shares to toss a coin
        # TODO check how often this is not the case
        n_collected = len(coin_shares)
        n_required = self.n_processes//3 + 1
        if len(coin_shares) == n_required:
            logger.info(f'Toss coin {self.process_id}: succeded -- {n_collected} out of required {n_required} shares collected.')
            return self.threshold_coins[fai][ind_dealer].combine_coin_shares(coin_shares)
        else:

            logger.info(f'Toss coin {self.process_id}: failed because only {n_collected} out of required {n_required} shares were collected :(.')
            return self._simple_coin(U_c, level)


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
        U_c_hash = U_c.hash()
        U_hash = U.hash()
        memo = self.timing_partial_results[U_c_hash]

        pi_value = memo.get(('pi', U_hash), None)
        if pi_value is not None:
            return pi_value

        r_value = self.r_function(U_c, U)

        if r_value == -1:
            if self.below(U_c, U):
                memo[('pi', U_hash)] = 1
                return 1
            else:
                memo[('pi', U_hash)] = 0
                return 0

        pi_values_level_below = []

        for V in self.get_all_prime_units_by_level(U.level-1):
            if self.high_below(V, U):
                pi_values_level_below.append(self.compute_pi(U_c, V))

        if r_value == 0:
            pi_value = self.exists_tc(pi_values_level_below, U_c, U)
        if r_value == 1:
            pi_value = self.super_majority(pi_values_level_below)

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

        r_value = self.r_function(U_c, U)

        if r_value == -1:
            return -1

        assert r_value == 0, "Delta is attempted to be evaluated at an odd level. This is unnecessary and should not be done."

        if r_value == 0:
            pi_values_level_below = []
            for V in self.get_all_prime_units_by_level(U.level-1):
                if self.high_below(V, U):
                    pi_values_level_below.append(self.compute_pi(U_c, V))
            delta_value = self.super_majority(pi_values_level_below)
            memo[('delta', U_hash)] = delta_value
            return delta_value


    def decide_unit_is_timing(self, U_c):
        # go over even levels starting from U_c.level + 2
        U_c_hash = U_c.hash()

        if U_c_hash not in self.timing_partial_results:
            self.timing_partial_results[U_c_hash] = {}

        memo = self.timing_partial_results[U_c_hash]
        if 'decision' in memo.keys():
            return memo['decision']

        for level in range(U_c.level + 2, self.level_reached + 1, 2):
            for U in self.get_all_prime_units_by_level(level):
                decision = self.compute_delta(U_c, U)
                if decision != -1:
                    if level == U_c.level + 2 and decision == 0:
                        # here is the exception -- a case in which Lemma 3.17 (ii) fails
                        # need to stay undecided here
                        pass
                    else:
                        memo['decision'] = decision
                        return decision
        return -1


    def decide_timing_on_level(self, level):
        # NOTE: this is perhaps not the most efficient way of doing it but it's arguably the cleanest
        # also, the redundant computations here are not that significant for the "big picture"
        sigma = self.crp[level]

        for process_id in sigma:
            prime_units_by_curr_process = self.prime_units_by_level[level][process_id]

            if len(prime_units_by_curr_process) == 0:
                # we have not seen any prime unit of this process at that level
                # there might still come one, so we need to wait, but no longer than till the level grows >= level+4
                # in which case a negative decision is guaranteed
                if self.level_reached >= level + 4:
                    #we can safely skip this process
                    continue
                else:
                    #no decision can be made, need to wait
                    return -1

            #TODO: the case when there are multiple units in this list is especially tricky (caused by forking)
            #TODO: there can be hidden conceptual bugs in here

            #In case there are multiple (more than one) units to consider (forking) we sort them by hashes (to break ties)
            prime_units_by_curr_process.sort(key = lambda U: U.hash())
            for U_c in prime_units_by_curr_process:
                decision = self.decide_unit_is_timing(U_c)
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
                # assert len(self.timing_units) == level, "The length of the list of timing units does not match the level of the currently added unit"
            else:
                # don't need to consider next level if there is already no timing unit chosen for the current level
                break
        if timing_established:
            self.level_timing_established = timing_established[-1].level

        return timing_established

    def extract_tcoin_from_dealing_unit(self, U, process_id):
        self.threshold_coins[U.creator_id].append(U.coin_shares[process_id])



    def add_threshold_coin(self, threshold_coin):
        self.threshold_coins[threshold_coin.dealer_id].append(threshold_coin)

#===============================================================================================================================
# HELPER FUNCTIONS LOOSELY RELATED TO POSETS
#===============================================================================================================================



    def order_units_topologically(self, units_list):
        '''
        Outputs a topological order of units_list.
        More formally it outputs a list top_list such that:
            whenever U, V are in units_list and V is a parent of U then V appears before U in top_list.
        Note: this does not necessarily preserve the ordering in the poset!
        :param int process_id: the identification number of a process
        :returns: list top_list: a topologically sorted list units_list
        '''
        # NOTE: this might be potentially slow, as it uses a set of Units
        # implements a DFS on a custom stack
        #hash_to_units = {U.hash():U for U in units_list}
        units_added = set()
        units_set = set(units_list)
        top_list = []
        #a unit on a stack is stored along with its color (0 or 1) depending on its state in the DFS algorithm
        unit_stack = []
        for U in units_list:
            if U not in units_added:
                unit_stack.append((U,0))
                units_added.add(U)

            while unit_stack:
                V, color = unit_stack.pop()
                if color == 0:
                    unit_stack.append((V,1))
                    for W in V.parents:
                        if W in units_set and W not in units_added:
                            unit_stack.append((W,0))
                            units_added.add(W)
                if color == 1:
                    top_list.append(V)

        return top_list



    def validate_using_new_unit(self, U):
        '''
        Validate as many units as possible using the newly-created unit U.
        Start from min_non_validated and continue towards the top.
        :returns: the set of all units first-time validated by U
        '''
        validated = []
        for process_id in range(self.n_processes):
            to_check = set(self.min_non_validated[process_id])
            non_validated = []
            while to_check:
                V = to_check.pop()
                # the first check below is for efficiency only
                if self.below(V,U) and self.high_below(V,U):
                    validated.append(V)
                    to_check = to_check.union(self.get_self_children(V))
                else:
                    non_validated.append(V)
            self.min_non_validated[process_id] = non_validated
        return validated


    def units_by_height(self, process_id, height):
        '''
        Returns list of units created by a given process of a given height.
        NOTE: this implementation is inefficient.
        In the future one could improve it by memoizing every k-th height of units, for some constant k, like k=100.
        '''
        if height < 0 or self.max_units_per_process[process_id] == []:
            return []

        if height < self.forking_height[process_id]:
            # we can use the memoized units, there will be a unique result
            # find the lowest memoized unit above height -- ceil(height/self.memo_height)
            memoized_pos = (height + self.memo_height - 1)//self.memo_height
            if len(self.memoized_units[process_id]) >= memoized_pos + 1:
                U = self.memoized_units[process_id][memoized_pos]
            else:
                U = self.max_units_per_process[process_id][0]

            if U.height < height:
                return []

            while U is not None and U.height > height:
                U = U.self_predecessor
            return [U]

        # we need to be especially careful because the query is about a fork
        # thus we go all the way from the top to not miss anything
        result_list = []
        for U in self.max_units_per_process[process_id]:
            if U.height < height:
                continue
            while U is not None and U.height > height:
                U = U.self_predecessor
            if U is not None and U.height == height:
                result_list.append(U)

        #remove possible duplicates -- process_id is a forker
        return list(set(result_list))



    def get_self_children(self, U):
        '''
        Returns the set of all units V in the poset such that V.self_predecessor == U
        NOTE: inefficient because units_by_height is inefficient.
        '''
        return self.units_by_height(U.creator_id, U.height + 1)


    def fix_parents(self, U):
        assert all(p in self.units for p in U.parents), 'Attempting to fix parents but parents not present in poset'
        U.parents = [self.units[p] for p in U.parents]


#===============================================================================================================================
# LINEAR ORDER
#===============================================================================================================================



    def break_ties(self, units_list):
        '''
        Break ties. Break them gooooood.
        I love the sound of breaking ties in the morning.
        '''
        R = reduce(xor, map(lambda x: x.hash(), units_list))
        #TODO: might be a good idea to precalculate those?
        tiebraker = lambda U: xor(R, U.hash())

        children = {U:[] for U in units_list} #lists of children
        parents  = {U:0  for U in units_list} #number of parents
        orphans  = set(units_list)
        for U in units_list:
            for P in U.parents:
                if P in children: #same as "if P in units_list", but faster
                    children[P].append(U)
                    parents[U] += 1
                    orphans.discard(U)

        ret = []

        while orphans:
            ret += sorted(orphans, key=tiebraker)

            out = list(orphans)
            orphans = set()
            for U in out:
                for child in children[U]:
                    parents[child] -= 1
                    if parents[child] == 0:
                        orphans.add(child)

        return ret



    def timing_round(self, k):
        '''
        Return a list of all units with timing round equal k.
        In other words, all U such that U < T_k but not U < T_(k-1) where T_i is the i-th timing unit.
        '''
        T_k = self.timing_units[k]
        T_k_1 = self.timing_units[k-1] if k > 0 else None

        ret = []
        Q = set([T_k])
        while Q:
            U = Q.pop()
            if T_k_1 is None or not self.below(U, T_k_1):
                ret.append(U)
                for P in U.parents:
                    Q.add(P)

        return ret




#===============================================================================================================================
# THE LAND OF UNUSED CODE AND WISHFUL THINKING ;)
#===============================================================================================================================



#    def find_maximum_within_process(self, units_list, process_id):
#        '''
#        Finds a unit U in units_list that is above (within process_id)
#        to all units in units_list.
#        :param list units_list: list of units created by process_id
#        :param int process_id: the identification number of a process
#        :returns: unit U that is the maximum of units_list or None if there are several incomparable maximal units.
#        '''
#        #NOTE: for efficiency reasons this returns None if there is no single "maximum"
#        #NOTE: this could be potentially confusing, but returning a set of "maximal" units instead
#        #NOTE: would add unnecessary computation whose result is anyway ignored later on
#
#        maximum_unit = None
#
#        for U in units_list:
#            assert (U.creator_id == process_id), "Expected a list of units created by process_id"
#            if maximum_unit is None:
#                maximum_unit = U
#                continue
#            if self.above_within_process(U, maximum_unit):
#                maximum_unit = U
#                continue
#            if not self.above_within_process(maximum_unit, U):
#                return None
#
#        return maximum_unit


#    def get_known_forkers(self, U):
#        '''
#        Finds all processes that can be proved forking given evidence in the lower-cone of unit U.
#        :param unit U: unit whose lower-cone should be considered for forking attempts
#        '''
#
#        # TODO: make sure this data structure is properly updated when new units are added!
#        # NOTE: this implementation assumes that U has been already added to the poset (data structure has been updated)
#
#        assert (U.hash() in self.known_forkers_by_unit.keys()), "Unit U has not yet been added to known_forkers_by_unit"
#
#        return self.known_forkers_by_unit[U.hash()]



    def rand_maximal(self):
        '''
        Returns a randomly chosen maximal unit in the poset.
        '''

        pass


    def my_maximal(self):
        '''
        Returns a randomly chosen maximal unit that is above a last created unit by this process.
        '''

        pass


    def get_prime_units(self):
        '''
        Returns the set of all prime units.
        '''

        pass


    def timing_units(self):
        '''
        Returns a set of all timing units.
        '''

        pass
