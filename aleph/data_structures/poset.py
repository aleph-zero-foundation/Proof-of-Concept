'''This module implements a poset - a core data structure.'''

from itertools import product

from aleph.data_structures.unit import Unit
from aleph.crypto.signatures.keys import PrivateKey, PublicKey
from .. import config


class Poset:
    '''This class is the core data structure of the Aleph protocol.'''


    def __init__(self, n_processes, process_id, secret_key, public_key, compliance_rules=None):
        '''
        :param int n_processes: the committee size
        :param int process_id: identification number of process whose local view is represented by this poset
        :param list compliance_rules: list of strings defining which compliance rules are followed by this poset
        '''
        self.n_processes = n_processes
        self.process_id = process_id
        self.compliance_rules = compliance_rules

        self.units = {}
        self.max_units_per_process = [[] for _ in range(n_processes)]
        self.forking_height = [float('inf')] * n_processes

        self.secret_key = secret_key
        self.public_key = public_key

        #self.level_reached = 0
        self.prime_units_by_level = {}

        # For every unit U maintain a list of processes that can be proved forking by looking at the lower-cone of U
        #self.known_forkers_by_unit = {}



#===============================================================================================================================
# UNITS
#===============================================================================================================================



    def add_unit(self, U):
        '''
        Adds a unit compliant with the rules, what was checked by check_compliance.
        This method does the following:
            1. adds the unit U to the poset,
            2. sets U's self_predecessor, height, and floor fields,
            3. updates ceil field of predecessors of U,
            4. updates the lists of maximal elements in the poset.
            5. adds an entry to known_forkers_by_unit

        :param unit U: unit to be added to the poset
        '''

        # TOTHINK: maybe we should do check_compliance here????

        self.units[U.hash()] = U

        U.floor = [[] for _ in range(self.n_processes)]
        self.update_floor(U)

        U.ceil = [[] for _ in range(self.n_processes)]
        U.ceil[U.creator_id] = [U]
        for parent in U.parents:
            self.update_ceil(U, parent)

        self.max_units_per_process[U.creator_id] = U

        ## 5. add an entry to known_forkers_by_unit
        #forkers = []
        # process_id is known forking if U.floor[process_id] has more than one element
        #for process_id in range(self.n_processes):
        #    if len(U.floor[process_id]) > 1:
        #        forkers.append(process_id)

        #self.known_forkers_by_unit[U.hash()] = forkers



    def create_unit(self, parents, txs):
        '''
        Creates a new unit and stores thx in it. Correctness of the txs is checked by a thread listening for new transactions.
        :param list parents: list of hashes of parent units
        :param list txs: list of correct transactions
        :returns: the new-created unit
        '''

        # NOTE: perhaps we (as an honest process) should always try (if possible)
        # NOTE: to create a unit that gives evidence of another process forking

        U = Unit(self.process_id, parents, txs)
        return U



    def sign_unit(self, U):
        '''
        Signs the unit.
        TODO This method should be probably a part of a process class which we don't have right now.
        '''

        message = str([U.creator_id, U.parents, U.txs, U.coinshares]).encode()
        U.signature = self.secret_key.sign(message)



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

        for V in self.get_prime_units_by_level(m):
            if self.high_below(V, U):
                processes_high_below += 1

        # same as (...)>=2/3*(...) but avoids floating point division
        U.level = m+1 if 3*processes_high_below >= 2*self.n_processes else m
        return U.level



    def check_primeness(self, U):
        '''
        Check if the unit is prime.
        :param unit U: the unit to be checked for being prime
        '''
        # U is prime iff it's a bottom unit or its self_predecessor level is strictly smaller
        return len(U.parents) == 0 or self.level(U) > self.level(U.self_predecessor)



    def get_prime_units_by_level(self, level):
        '''
        Returns the set of all prime units at a given level.
        :param int level: the requested level of units
        '''
        # TODO: this is a naive implementation
        # TODO: make sure that at creation of a prime unit it is added to the dict self.prime_units_by_level
        return self.prime_units_by_level[level]



    def set_self_predecessor_and_height(self, U):
        '''
        Computes the self_predecessor of a unit U and fills in the appropriate field in U.
        :param unit U: unit whose self_predecessor is being computed
        '''
        if len(U.parents) == 0:
            U.self_predecessor = None
            U.height = 0
        else:
            combined_floors = self.combine_floors_per_process(U.parents, U.creator_id)
            assert (len(combined_floors) >= 1), "Unit U has no candidates for predecessors."
            assert (len(combined_floors) <= 1), "Unit U has more than one candidate for predecessor."
            U.self_predecessor = combined_floors[0]
            U.height = U.self_predecessor.height + 1



#===============================================================================================================================
# COMPLIANCE
#===============================================================================================================================



    def check_compliance(self, U):
        '''
        Checks if the unit U is correct and follows the rules of creating units, i.e.:
            1. Parents of U are correct (exist in the poset, etc.)
            2. Has correct signature.
            3. Satisfies anti-fork policy.
            4. Satisfies parent diversity rule.
            5. Check "growth" rule.
            6. The coinshares are OK, i.e., U contains exactly the coinshares it is supposed to contain.
        :param unit U: unit whose compliance is being tested
        '''
        # TODO: there might have been other compliance rules that have been forgotten...
        # TODO: should_check() with string arguments is ugly. This is a temporary solution

        should_check = lambda x: (self.compliance_rules is None or x in self.compliance_rules)

        # 1. Parents of U are correct.
        if not self.check_parent_correctness(U):
            return False

        # 2. Has correct signature.
        if not self.check_signature_correct(U):
            return False

        # 3. Satisfies anti-fork policy.
        if should_check('anti_fork') and not self.check_anti_fork(U):
            return False

        # At this point we know that U has a well-defined self_predecessor
        # We can set it -- needed for subsequent checks
        self.set_self_predecessor_and_height(U)

        # 4. Satisfies parent diversity rule.
        if should_check('parent_diversity') and not self.check_parent_diversity(U):
            return False

        # 5. Check "growth" rule.
        if should_check('growth') and not self.check_growth(U):
            return False

        # 6. Coinshares are OK.
        # TODO: implementation missing

        return True



    def check_growth(self, U):
        '''
        Checks if the unit U, created by process j, respects the "growth" rule.
        Suppose U wants to use a unit V as its parent and let i (not equal to j) be the creator of V.
        Let U_previous be the highest ancestor of U, created by j
        that has as a parent a unit V_previous created by i.
        Then, we force V_previous < V (strictly less than).
        :param unit U: unit that is tested against the grow rule
        :returns: Boolean value, True if U respects the rule, False otherwise.
        '''

        if len(U.parents) == 0:
            return True

        # U.self_predecessor should be correctly set when invoking this method
        assert (U.self_predecessor is not None), "The self_predecessor field has not been filled for U"

        for V in U.parents:
            if V.creator_id == U.creator_id:
                continue
            floor_predecessor = U_self_predecessor.floor[V.creator_id]

            assert (len(floor_predecessor) <= 1), "The creator of V is known to be forking, it should have been rejected before."

            if len(floor_predecessor) == 0:
                # this means that the creator of U never linked to V.creator_id before
                pass

            elif len(floor_predecessor) == 1:
                V_previous = floor_predecessor[0]
                if not self.strictly_below_within_process(V_predecessor, V):
                    return False

        return True



    def check_anti_fork(self, U):
        '''
        Checks if the unit U respects the anti-forking policy, i.e.:
        The following situations A), B) are not allowed:
        A)
            - There exists a process j, s.t. one of U's parents was created by j
            AND
            - U has as one of the parents a unit that has evidence that j is forking.
        B)
            - The creator of U is the process j
            AND
            - The parents of U (combined) have evidence that j is forking

        :param unit U: unit that is checked for respecting anti-forking policy
        :returns: Boolean value, True if U respects the policy, False otherwise.
        '''

        if len(U.parents) == 0:
            return True

        # Check for situation A)
        parent_processes = set([V.creator_id for V in U.parents])
        for V, proc in product(U.parents, parent_processes):
            if self.has_forking_evidence(V, proc):
                return False

        # Check for situation B)
        combined_floors = self.combine_floors_per_process(U.parents, U.creator_id)
        if len(combined_floors) > 1:
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

        # Special case: U is a bottom unit
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

            if n_parent_processes*3 >= self.n_procesees:
                break

            if proposed_parent_process_occurence:
                # a proposed parent process repeated too early!
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
        V.ceil.append(U)
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
        index = lenghts.index(max(lengths))
        forks = units_lists[index].floor[process_id][:]

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



    def has_forking_evidence(self, unit, process_id):
        '''
        Checks if a unit has in its lower cone an evidence that process_id is forking.
        :param unit unit: unit to be checked for evidence of process_id forking
        :param int process_id: identification number of process to be verified
        :returns: True if forking evidence is present, False otherwise
        '''
        return len(unit.floor[process_id]) > 1



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
        assert (U.creator_id == V.creator_id and U.creator_id is not None) , "expected two processes created by the same process"
        if U.height > V.height:
            return False
        process_id = U.creator_id
        # if process_id is non-forking or at least U is below the process_id's forking level then clearly U has a path to V
        if U.height <= self.forking_height[process_id]:
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
        return self.below_within_process(self, V, U)



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


    def choose_coinshares(self, unit):
        '''
        Implements threshold_coin algorithm from the paper.
        '''

        pass



    def diff(self, other):
        '''
        Returns a set of units that are in this poset and that are not in the other poset.
        '''

        pass



    def unit_by_height(self, process_id, height):
        '''
        Returns a unit or a list of units created by a given process of a given height.
        '''

        pass



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


