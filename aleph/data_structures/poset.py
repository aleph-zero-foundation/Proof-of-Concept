'''This module implements a poset - a core data structure.'''

from itertools import product
from functools import reduce
import random
import logging


from aleph.crypto.signatures.threshold_signatures import generate_keys, SecretKey, VerificationKey
from aleph.crypto.threshold_coin import ThresholdCoin
from aleph.crypto import sha3_hash
from aleph.crypto.byte_utils import extract_bit

from aleph.data_structures.unit import Unit

import aleph.const as consts


class Poset:
    '''This class is the core data structure of the Aleph protocol.'''

    def __init__(self, n_processes, process_id = None, crp = None, use_tcoin = None,
                compliance_rules = None):
        '''
        :param int n_processes: the committee size
        :param list compliance_rules: dictionary string -> bool
        '''
        self.n_processes = n_processes
        self.default_compliance_rules = {'forker_muting': True, 'parent_diversity': False, 'growth': False, 'expand_primes': True, 'threshold_coin': use_tcoin}
        self.compliance_rules = compliance_rules
        self.use_tcoin = use_tcoin if use_tcoin is not None else consts.USE_TCOIN
        # process_id is used only to support tcoin (i.e. in case self.use_tcoin = True), to know which shares to add and which tcoin to pick from dealing units
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
        # threshold coins dealt, this is a dictionary {Unit_hash -> ThresholdCoin} where keys are hashes of dealing units
        self.threshold_coins = {}

        self.prime_units_by_level = {}

        # The list of dealing units for every process -- in a healthy situation (absence of forkers) there should be one per process
        self.dealing_units = [[] for _ in range(n_processes)]

        #timing units
        self.timing_units = []

        #a structure for memoizing partial results about the computation of pi/delta
        # it has the form of a dict with keys being unit hashes (U_c.hash) and values being dicts indexed by pairs (fun, U.hash)
        # whose value is the memoized value of computing fun(U_c, U) where fun in {pi, delta}
        self.timing_partial_results = {}

        #we maintain a list of units in the poset ordered according to when they were added to the poset -- necessary for dumping the poset to file
        self.units_as_added = []


#===============================================================================================================================
# UNITS
#===============================================================================================================================

    def prepare_unit(self, U):
        '''
        Sets basic fields of U; should be called prior to check_compliance and add_unit methods.
        This method does the following:
            0. set floor field
            1. set U's level
        '''

        # 0. set floor field
        U.floor = [[] for _ in range(self.n_processes)]
        self.update_floor(U)

        # 1. set U's level
        U.level = self.level(U)


    def add_unit(self, U):
        '''
        Add a unit compliant with the rules, what was checked by check_compliance.
        This method does the following:
            0. add the unit U to the poset
            1. if it is a dealing unit, add it to self.dealing_units
            2. update the lists of maximal elements in the poset.
            3. update forking_height
            4. if U is prime, add it to prime_units_by_level
        :param unit U: unit to be added to the poset
        '''

        # 0. add the unit U to the poset
        assert U.level is not None, "Level of the unit being added is not computed."

        self.level_reached = max(self.level_reached, U.level)
        self.units[U.hash()] = U
        self.units_as_added.append(U)

        # if it is a dealing unit, add it to self.dealing_units
        if not U.parents and not U in self.dealing_units[U.creator_id]:
            self.dealing_units[U.creator_id].append(U)
            # extract the corresponding tcoin black box (this requires knowing the process_id)
            if self.use_tcoin:
                assert self.process_id is not None, "Usage of tcoin enable but process_id not set."
                self.extract_tcoin_from_dealing_unit(U)


        # 2. updates the lists of maximal elements in the poset and forking height
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
            # NOTE: we need to make sure that there is a deterministic order of units on the self.prime_units_by_level[U.level][U.creator_id] list
            #       in case of forks there is more than one unit on this list and for consensus reason it becomes important to traverse
            #       units on this list in exactly the same order by every process
            self.prime_units_by_level[U.level][U.creator_id].sort(key = lambda U_x: U_x.hash())



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

        # need to count all processes that produced a unit V of level m such that U'<U
        # we can limit ourselves to prime units V
        processes_below = 0

        for process_id in range(self.n_processes):
            Vs = self.prime_units_by_level[m][process_id]
            for V in Vs:
                if self.below(V, U) and (V is not U):
                    processes_below += 1
                    break

            # For efficiency:
            if 3*(processes_below + self.n_processes - 1 - process_id) < 2*self.n_processes:
                break

        # same as (...)>=2/3*(...) but avoids floating point division
        U.level = m+1 if 3*processes_below >= 2*self.n_processes else m
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

        # We start adding coin shares only at levels >= consts.ADD_SHARES, this implies
        #    that with rather high probability there will be no more than a (small) constant (likely = 1) number of shares per unit
        if U.level < consts.ADD_SHARES:
            return []

        # the to-be-constructed list of pairs of indices such that for (i,j) in the list the coin share TC^j_i(L(U)) should be added to U
        indices = []

        # don't add coin shares of dealers that are proved by U to be forkers or U does not see a dealing unit
        skip_dealer_ind = set(dealer_id for dealer_id in range(self.n_processes) if self.has_forking_evidence(U, dealer_id) or
                                                                                    self.index_dealing_unit_below(dealer_id, U) is None)

        share_id = U.creator_id

        # starting from level 3 there is negligible probability that the transversal will have more than 1 element
        # as we start adding coin shares to units of level 6, everything is just fine
        level = consts.ADD_SHARES

        # construct the list of all prime units below U at level ADD_SHARES (or higher, if no unit at level=ADD_SHARES for a given process)
        # it can be proved that these are enough instead of *all* prime units at levels ADD_SHARES <= ... <= U.level - 1
        prime_below_U = []
        for process_id in range(self.n_processes):
            Vs = self.get_prime_unit_above_level(process_id, level)
            if Vs and Vs[0].level <= U.level - 1:
                prime_below_U += [V for V in Vs if self.below(V,U)]


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
        if U.level >= consts.ADD_SHARES:
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
        # create a dict of all VKs and SKs in a raw format -- charm group elements with no classes wrapped around them
        cs_dict = {}
        vk, sks = generate_keys(self.n_processes, self.n_processes//3+1)
        cs_dict['vk'] = vk.vk
        # TODO: these should be encrypted using corresponding processes' public keys
        cs_dict['sks'] = [secret_key.sk for secret_key in sks]
        cs_dict['vks'] = vk.vks
        U.coin_shares = cs_dict


    def get_all_prime_units_by_level(self, level):
        '''
        Returns the set of all prime units at a given level.
        :param int level: the requested level of units
        '''
        if level not in self.prime_units_by_level.keys():
            return []
        return [V for Vs in self.prime_units_by_level[level] for V in Vs]

    def get_prime_units_at_level_below_unit(self, level, U):
        '''
        Returns the set of all prime units at a given level that are below the unit U.
        :param int level: the requested level of units
        :param unit U: the unit below which we want the prime units
        '''
        return [V for V in self.get_all_prime_units_by_level(level) if self.below(V, U)]

    def get_prime_units_by_level_per_process(self, level):
        '''
        Returns the set of all prime units at a given level.
        :param int level: the requested level of units
        '''
        # TODO: this is a naive implementation
        # TODO: make sure that at creation of a prime unit it is added to the dict self.prime_units_by_level
        assert level in self.prime_units_by_level.keys()
        return self.prime_units_by_level[level]


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
            2. U does not provide evidence of its creator forking
            3. Satisfies forker-muting policy.
            4. Satisfies parent diversity rule. (optionally)
            5. Check "growth" rule. (optionally)
            6. Satisfies the expand primes rule.
            7. The coinshares are OK, i.e., U contains exactly the coinshares it is supposed to contain.
        :param unit U: unit whose compliance is being tested
        '''
        # TODO: it is highly desirable that there are no duplicate transactions in U (i.e. literally copies)

        # 1. Parents of U are correct.
        if not self.check_parent_correctness(U):
            return False

        if len(U.parents) == 0:
            # This is a dealing unit, and its signature is correct --> we only need to check whether threshold coin is included
            if self.use_tcoin and not self.check_threshold_coin_included(U):
                return False
            else:
                return True

        # 2. U does not provide evidence of its creator forking
        if not self.check_no_self_forking_evidence(U):
            return False

        # 3. Satisfies forker-muting policy.
        if self.should_check_rule('forker_muting'):
            if not self.check_forker_muting(U):
                return False

        # 4. Satisfies parent diversity rule.
        if self.should_check_rule('parent_diversity'):
            if not self.check_parent_diversity(U):
                return False

        # 5. Check "growth" rule.
        if self.should_check_rule('growth'):
            if not self.check_growth(U):
                return False

        # 6. Sastisfies the expand primes rule
        if self.should_check_rule('expand_primes'):
            if not self.check_expand_primes(U):
                return False

        # 7. Coinshares are OK.
        if self.should_check_rule('threshold_coin'):
            if self.is_prime(U) and not self.check_coin_shares(U):
                return False

        return True

    def check_threshold_coin_included(self, U):
        '''
        Checks whether the dealing unit U has a threshold coin included.
        We cannot really check whether it is valid (since the secret keys are encrypted).
        Instead, we simply make sure whether the dictionary has all necessary fields and the corresponding lists are of appropriate length.
        :returns: Boolean value, True if U's threshold coin is correct, False otherwise.
        '''
        if not isinstance(U.coin_shares, dict):
            return False

        # coin_shares['vk'] should be the "cumulative" public key
        if not 'vk' in U.coin_shares:
            return False

        # coin_shares['vks'] should be a list of public keys for every process
        if not 'vks' in U.coin_shares or len(U.coin_shares['vks']) != self.n_processes:
            return False

        # coin_shares['sks'] should be a list of private keys for every process
        if not 'sks' in U.coin_shares or len(U.coin_shares['sks']) != self.n_processes:
            return False

        # TODO: one should also check whether vks and vk represent valid elements of the PAIRINGGROUP

        return True




    def check_no_self_forking_evidence(self, U):
        '''
        Checks if the unit U does not provide evidence of its creator forking.
        :param unit U: the unit whose forking evidence is being checked
        :returns: Boolean value, True if U does not provide evidence of its creator forking
        '''
        combined_floors = self.combine_floors_per_process(U.parents, U.creator_id)
        if len(combined_floors) == 1:
            return True
        else:
            return False

    def check_expand_primes(self, U):
        '''
        Checks if the unit U respects the "expand primes" rule.
        Let L be the level of U's predecessor and P the set of
        prime units of level L below the predecessor.
        Every parent of U that is not its predecessor must have
        more prime units of level L below it than all the previous
        parents combined.
        :param unit U: unit that is tested against the expand primes rule
        :returns: Boolean value, True if U respects the rule, False otherwise.
        '''
        # Special case of dealing units
        if len(U.parents) == 0:
            return True

        level = U.self_predecessor.level

        prime_below_parents = set(self.get_prime_units_at_level_below_unit(level, U.self_predecessor))
        for V in U.parents[1:]:
            # if the level of V is higher, accept it and check everything from here at that level
            if V.level > level:
                level = V.level
                prime_below_parents = set()
            prime_below_V = set(self.get_prime_units_at_level_below_unit(level, V))
            # If V has only a subset of previously seen prime units below it we have a violation
            if prime_below_V <= prime_below_parents:
                return False
            # Add the new prime units to seen units
            prime_below_parents.update(prime_below_V)

        return True

    def check_growth(self, U):
        '''
        Checks if the unit U, created by process j, respects the "growth" rule.
        No parent of U can be below the self predecessor of U.
        :param unit U: unit that is tested against the grow rule
        :returns: Boolean value, True if U respects the rule, False otherwise.
        '''
        if len(U.parents) == 0:
            return True

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

    def check_parent_correctness(self, U):
        '''
        Checks whether U has correct parents:
        0. Parents of U exist in the poset
        1. The first parent was created by U's creator and has one less height than U.
        2. If U has >=2 parents then all parents are created by pairwise different processes.
        :param unit U: unit whose parents are being checked
        '''
        # 0. Parents of U exist in the poset
        for V in U.parents:
            if V.hash() not in self.units.keys():
                return False

        # 1. The first parent was created by U's creator and has one less height than U.
        alleged_predecessor = U.self_predecessor
        if alleged_predecessor is not None:
            if alleged_predecessor.creator_id != U.creator_id or alleged_predecessor.height + 1 != U.height:
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

    def validate_share(self, U, share_index, dealer_id):
        '''
        Checks whether a particular coin share agrees with the dealt public key.
        Note that even if it does not, it does not follow that U.creator_id is adversary -- it could be that dealer_id is the cheater.
        '''
        ind = self.index_dealing_unit_below(dealer_id, U)
        assert ind is not None
        coin_share = U.coin_shares[share_index]
        return self.threshold_coins[dealer_id][ind].verify_coin_share(coin_share, U.creator_id, U.level)


    def check_coin_shares(self, U):
        '''
        Checks if coin shares stored in U "are OK".
        :param unit U: unit which coin shares are checked
        :returns: True if everything works and False otherwise
        '''

        # NOTE: we cannot require that all the coinshares in U agree with the corresponding VK's dealt!
        # NOTE: this is because the corresponding SK (which is encrypted) could be broken, in which case U.creator_id could not generate correct shares
        # Instead, we require that the list of shares is at least of correct length...

        # TODO (not sure this should be done here, if at all): check if shares are valid, i.e. if they can be combined

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
            # NOTE: the check below cannot be performed, since U.creator_id has no control over the correctness of the tc dealt by dealer_id
            #if not self.threshold_coins[dealer_id][ind].verify_coin_share(coin_share, share_id, U.level):
            #    return False

        return True


#===============================================================================================================================
# FLOOR
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
        # unless of course U or is a fork that hasn't yet been added
        if U.height < self.forking_height[process_id] and U.hash() in self.units and V.hash() in self.units:
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

#===============================================================================================================================
# TIMING
#===============================================================================================================================

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
                    logger.info(f'decide_timing {process_id} | Timing unit for lvl {U_c.level} decided at lvl + {level - U_c.level}'
                                f', poset lvl + {self.level_reached - U_c.level}')
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
                        logger.info(f'decide_timing {process_id} | Timing unit for lvl {U_c.level} decided at lvl + {level - U_c.level}'
                                    f', poset lvl + {self.level_reached - U_c.level}')

                    return decision

        # Switch to the pi-delta algorithm if consensus could not be reached using the "fast algorithm".
        # It guarantees termination after a finite number of levels with probability 1.
        # Note that this piece of code will only execute if there is still no decision on U_c and level_reached is >= U_c.level + t_p_d,
        #   which we consider rather unlikely to happen since under normal circumstances (no malicious adversary) the fast algorithm
        #   will likely decide at level <= +5. The default value of t_p_d is 12, thus after reaching level +6 and assuming that default_vote
        #   is a random function of level, the probability of reaching level 12 is <= 2^{-7} <= 10^{-2}.
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
                        logger.info(f'decide_timing {process_id} | Timing unit for lvl {U_c.level} decided at lvl + {level - U_c.level}'
                                    f', poset lvl + {self.level_reached - U_c.level}')
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



#===============================================================================================================================
# PI AND DELTA FUNCTIONS
#===============================================================================================================================


    def exists_tc(self, list_vals, U_c, U_tossing):
        if 1 in list_vals:
            return 1
        if 0 in list_vals:
            return 0
        return self.toss_coin(U_c, U_tossing)


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
        # Note that level U_c.level + consts.PI_DELTA_LEVEL has number 1 because we want it to execute an "odd" round
        r = U.level - (U_c.level + consts.PI_DELTA_LEVEL) + 1
        assert r >= 1, "The pi_delta protocol is attempted on a too low of a level."
        U_c_hash = U_c.hash()
        U_hash = U.hash()
        memo = self.timing_partial_results[U_c_hash]

        pi_value = memo.get(('pi', U_hash), None)
        if pi_value is not None:
            return pi_value

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
            pi_value = self.exists_tc(votes_level_below, U)
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
        r = U.level - (U_c.level + consts.PI_DELTA_LEVEL) + 1

        assert r % 2 == 0, "Delta is attempted to be evaluated at an odd level."

        pi_values_level_below = []
        for V in self.get_all_prime_units_by_level(U.level-1):
            if self.below(V, U):
                pi_values_level_below.append(self.compute_pi(U_c, V))

        delta_value = self.super_majority(pi_values_level_below)
        memo[('delta', U_hash)] = delta_value
        return delta_value


    def _simple_coin(self, U, level):
        # Needs to be a deterministic function of (U, level).
        # We choose it to be the l'th bit of the hash of U where l := level % (n_bits_in_U_hash)
        l = level % (8 * len(U.hash()))
        return extract_bit(U.hash(), l)


    def first_dealing_unit(self, V):
        '''
        Returns the first dealing unit (sorted w.r.t. crp at level level(V)) that is below V.
        '''
        permutation = self.crp[self.level(V)]

        for dealer_id in permutation:
            if self.has_forking_evidence(V, dealer_id):
                continue
            for U in self.dealing_units[dealer_id]:
                if self.below(U,V):
                    return U

        #This is clearly a problem... Should not happen
        assert False, "No unit available for first_dealing_unit."
        return None


    def validate_share(self, U):
        '''
        Checks whether the coin share of U agrees with the dealt public key.
        Note that even if it does not, it does not follow that U.creator_id is adversary -- it could be that dealer_id is the cheater.
        '''
        U_dealing = self.first_dealing_unit(U)
        coin_share = U.coin_shares[0]
        return self.threshold_coins[U_dealing.hash()].verify_coin_share(coin_share, U.creator_id, U.level)


    def toss_coin(self, U_c, U_tossing):
        # The coin toss at unit U_tossing (necessarily at level >= consts.ADD_SHARES + 1)
        # With low probability the toss may fail -- typically because of adversarial behavior of some process(es).
        # :param unit U_c: the unit whose popularity decision is being considered by tossing a coin
        #                  this param is used only in case when the _simple_coin is used, otherwise
        #                  the result of coin toss is meant to be a function of U_tossing.level only
        # :param unit U_tossing: the unit what is cossing a toin
        # :returns: One of {0, 1} -- a (pseudo)random bit, impossible to predict before (U_tossing.level - 1) was reached

        logger = logging.getLogger(consts.LOGGER_NAME)
        logger.info(f'toss_coin_start | Tossing at lvl {U_tossing.level} for unit {U_c.short_name()} at lvl {U_c.level}.')

        if self.use_tcoin == False or consts.ADD_SHARES >= U_tossing.level:
            return self._simple_coin(U_tossing, U_tossing.level)

        level = U_tossing.level-1

        coin_shares = {}

        # the dealing unit is (hopefully) uniquely defined FDU of prime ancestors of U_tossing
        U_dealing = None

        # run through all prime ancestors of U_tossing to gather coin shares
        for V in self.get_all_prime_units_by_level(level):
            # we gathered enough coin shares -- ceil(n_processes/3)
            if len(coin_shares) == self.n_processes//3 + 1:
                break

            # can use only shares from units visible from the tossing unit (so that every process arrives at the same result)
            if not self.below(V, U_tossing):
                continue

            # the below check is necessary if V.creator_id is a forker -- we do not want to collect the same share twice
            if V.creator_id in coin_shares:
                continue

            fdu_V = self.first_dealing_unit(V)
            if U_dealing is None:
                U_dealing = fdu_V

            if U_dealing is not fdu_V:
                # two prime ancestors of U_tossing have different fdu's, this might cause a coin toss to fail
                # we do not abort yet, hoping that there will be enough coin shares by coin_dealer to toss anyway
                continue

            if V.coin_shares != []:
                # it is now guaranteed that V.coin_shares = [cs], because this list contains at most one element
                # check if the share is correct, it might be incorrect even if V.creator_id is not a cheater
                if self.validate_share(V):
                    coin_shares[V.creator_id] = V.coin_shares[0]


        # check whether we have enough valid coin shares to toss a coin
        n_collected = len(coin_shares)
        n_required = self.n_processes//3 + 1
        if n_collected == n_required:
            # this is the threshold coin we shall use
            t_coin = self.threshold_coins[U_dealing.hash()]
            coin, correct = t_coin.combine_coin_shares(coin_shares, str(level))
            if correct:
                logger.info(f'toss_coin_succ {self.process_id} | Succeded - {n_collected} out of required {n_required} shares collected')
                return coin
            else:
                logger.warning(f'toss_coin_fail {self.process_id} | Failed - {n_collected} out of required {n_required} shares collected, but combine unsuccesful')
                return self._simple_coin(U_c, level)

        else:
            logger.warning(f'toss_coin_fail {self.process_id} | Failed - {n_collected} out of required {n_required} shares were collected')
            return self._simple_coin(U_c, level)


    def add_coin_shares(self, U):
        '''
        Adds coin shares to the prime unit U using the simplified strategy: add the coin_share determined by FAI(U, U.level) to U
        :param unit U: prime unit to which coin shares are added
        '''

        coin_shares = []

        U_dealing = self.first_dealing_unit(U)
        coin_shares = [ self.threshold_coins[U_dealing.hash()].create_coin_share(U.level) ]
        # The coin_shares are in fact a one-element list, except when something goes wrong with decrypting the tcoin
        #   in the dealing unit. In the current version it cannot happen, as the tcoins are not encrypted.
        U.coin_shares = coin_shares


    def extract_tcoin_from_dealing_unit(self, U):
        assert U.parents == [], "Trying to extract tcoin from a non-dealing unit."
        threshold = self.n_processes//3+1
        sk = SecretKey(U.coin_shares['sks'][self.process_id])
        vk = VerificationKey(threshold, U.coin_shares['vk'], U.coin_shares['vks'])
        self.threshold_coins[U.hash()] = ThresholdCoin(U.creator_id, self.process_id, self.n_processes, threshold, sk, vk)


    def check_coin_shares(self, U):
        '''
        Checks coin shares of a prime unit that is not a dealing unit.
        This boils down to checking if U has exactly one share if its level is >= consts.ADD_SHARES and zero shares otherwise.
        At this point there is no point checking whether the share is correct, because that might be because of an dishonest dealer.
        '''
        assert self.is_prime(U), "Trying to check shares of a non-prime unit."
        assert len(U.parents) > 0, "Trying to check shares of a dealing unit."
        if self.level(U) < consts.ADD_SHARES:
            return len(U.coin_shares) == 0
        else:
            return len(U.coin_shares) == 1

#===============================================================================================================================
# HELPER FUNCTIONS LOOSELY RELATED TO POSETS
#===============================================================================================================================


    def get_prime_unit_above_level(self, process_id, level):
        '''
        Let L be the smallest level s.t. L>=level and there exists a prime unit created by process_id at level l.
        Then this outputs the list of all prime units created by process_id at level L.
        :param int process_id: the id number of a process
        :param int level: the target level
        :returns: List of prime units by process id with level = level (or higher if no such exists), or None
        '''
        # NOTE: this implementation is efficient only if level is relatively small
        # this assumption is realistic since it is normally called for level = 3
        # if there is no unit at lvl level yet, return None
        if not level in self.prime_units_by_level:
            return None

        # there is a unit/units by this process at this level, just return it
        if self.prime_units_by_level[level][process_id] != []:
            return self.prime_units_by_level[level][process_id]

        # need to find the lowest level above 'level' that has a unit created by process_id
        for U in self.max_units_per_process[process_id]:
            if U.level >= level:
                V = U
                # go down as long as we are above 'level'
                while V.self_predecessor.level >= level:
                    V = V.self_predecessor

                return self.prime_units_by_level[V.level][process_id]

        return None


#===============================================================================================================================
# LINEAR ORDER
#===============================================================================================================================


    def break_ties(self, units_list):
        '''
        Produce a linear ordering on the provided units.
        It is a slightly different implementation than in the arxiv paper to avoid using xor which turns out to be very slow in python.
        Essentially a sha3 hash is used in place of xor.
        :param list units_list: the units to be sorted, should be all units with a given timing round
        :returns: the same set of units in a list ordered linearly
        '''

        # R is a value that depends on all the units in units_list and does not depend on the order of units in units_list

        R = sha3_hash(b''.join(sorted(U.hash() for U in units_list)))

        children = {U:[] for U in units_list} #lists of children
        parents  = {U:0  for U in units_list} #number of parents
        # instead of xor(U.hash(), R) we hash the pair using sha3
        tiebreaker = {U: sha3_hash(U.hash() + R) for U in units_list}
        orphans  = set(units_list)
        for U in units_list:
            for P in U.parents:
                if P in children: #same as "if P in units_list", but faster
                    children[P].append(U)
                    parents[U] += 1
                    orphans.discard(U)

        ret = []

        while orphans:
            ret += sorted(orphans, key= lambda x: tiebreaker[x])

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
# DUMPING POSET TO FILE
#===============================================================================================================================


    def dump_to_file(self, file_name):
        '''
        Dumps the poset to file in a rather simple format. Units are listed in the same order as the were added to the poset.
        Except from parents and creator_id we also include info about the level of each unit and a bit 0/1 whether the unit was a timing unit.
        '''
        set_timing_units = set(self.timing_units)
        with open(file_name, 'w') as f:
            f.write(f'process_id {self.process_id}\n')
            f.write(f'n_units {self.process_id}\n')
            for U in self.units_as_added:
                f.write(f'{U.short_name()} {U.creator_id}\n')
                f.write('parents '+' '.join(V.short_name() for V in U.parents) + '\n')
                is_timing = (self.is_prime(U)) and (U in set_timing_units)
                f.write(f'level {self.level(U)}\n')
                f.write(f'timing {int(is_timing)}\n')
