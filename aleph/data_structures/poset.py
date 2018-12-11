'''This module implements a poset - a core data structure.'''

from itertools import product
import random
import logging

from aleph.data_structures.unit import Unit
from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.config import *


class Poset:
    '''This class is the core data structure of the Aleph protocol.'''


    def __init__(self, n_processes, compliance_rules=None):
        '''
        :param int n_processes: the committee size
        :param list compliance_rules: list of strings defining which compliance rules are followed by this poset
        '''
        self.n_processes = n_processes
        self.compliance_rules = compliance_rules

        self.units = {}
        self.max_units_per_process = [[] for _ in range(n_processes)]
        self.min_non_validated = [[] for _ in range(n_processes)]
        self.forking_height = [float('inf')] * n_processes

        #self.level_reached = 0
        self.prime_units_by_level = {}



        # For every unit U maintain a list of processes that can be proved forking by looking at the lower-cone of U
        #self.known_forkers_by_unit = {}



#===============================================================================================================================
# UNITS
#===============================================================================================================================



    def add_unit(self, U, newly_validated = None):
        '''
        Add a unit compliant with the rules, what was checked by check_compliance.
        This method does the following:
            0. set U's self_predecessor, height, and floor fields (temporary!)
            1. add the unit U to the poset,
            2. update the lists of maximal elements in the poset.
            3. update forking_height
            3. set floor attribute of U
            3. set ceil attribute of U and update ceil of predecessors of U
            6. validates units using U if possible and updates the border between validated and non-validated units

        :param unit U: unit to be added to the poset
        :returns: It does not return anything explicitly but modifies the newly_validated list: adds the units validated by U
        '''

        # TOTHINK: maybe we should do check_compliance here????

        # TODO: calling this function here is only a temporary solution for initial tests
        self.set_self_predecessor_and_height(U)

        self.units[U.hash()] = U

        # 2. updates the lists of maximal elements in the poset and
        # 3. update forking_height
        if len(U.parents) == 0:
            assert self.max_units_per_process[U.creator_id] == [], "A second dealing unit is attempted to be added to the poset"
            self.max_units_per_process[U.creator_id] = [U]
        else:
            if U.self_predecessor in self.max_units_per_process[U.creator_id]:
                self.max_units_per_process[U.creator_id].remove(U.self_predecessor)
                self.max_units_per_process[U.creator_id].append(U)
            else:
                # a new fork is detected
                self.max_units_per_process[U.creator_id].append(U)
                self.forking_height[U.creator_id] = min(self.forking_height[U.creator_id], U.height)

        U.floor = [[] for _ in range(self.n_processes)]
        self.update_floor(U)

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




    def create_unit(self, creator_id, txs, strategy = "link_self_predecessor", num_parents = 2):
        '''
        Creates a new unit and stores txs in it. Correctness of the txs is checked by a thread listening for new transactions.
        :param list txs: list of correct transactions
        :param string strategy: strategy for parent selection, one of:
        - "link_self_predecessor"
        - "link_above_self_predecessor"
        :param int num_parents: number of distinct parents
        :returns: the new-created unit, or None if it is not possible to create a compliant unit
        '''

        # NOTE: perhaps we (as an honest process) should always try (if possible)
        # NOTE: to create a unit that gives evidence of another process forking
        logger = logging.getLogger(LOGGING_FILENAME)
        U = Unit(creator_id, [], txs)
        logger.info(f"create: {creator_id} attempting to create a unit.")
        #print(f"create: {creator_id} attempting to create a unit.")
        if len(self.max_units_per_process[creator_id]) == 0:
            # this is going to be our dealing unit

            logger.info(f"create: {creator_id} created its dealing unit.")
            return U


        assert len(self.max_units_per_process[creator_id]) == 1, "It appears we have created a fork."
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

        return U



    #def sign_unit(self, U):
    #    '''
    #    Signs the unit.
    #    TODO This method should be probably a part of a process class which we don't have right now.
    #    '''

    #    message = str([U.creator_id, U.parents, U.txs, U.coinshares]).encode()
    #    U.signature = self.secret_key.sign(message)



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
            U = self.units[U]
            if U_hash not in prev_hashes and U_hash not in diff_hashes:
                diff_hashes.add(U_hash)
                if U.self_predecessor is not None:
                    curr_hashes.add(U.self_predecessor.hash())

        return [self.units[U_hash] for U_hash in diff_hashes]








#===============================================================================================================================
# COMPLIANCE
#===============================================================================================================================



    def check_compliance(self, U):
        '''
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
        # TODO: there might have been other compliance rules that have been forgotten...
        # TODO: should_check() with string arguments is ugly. This is a temporary solution
        # TODO: it is highly desirable that there are no duplicate transactions in U (i.e. literally copies)

        should_check = lambda x: (self.compliance_rules is None or x in self.compliance_rules)

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
        if should_check('forker_muting') and not self.check_forker_muting(U):
            return False

        # 5. Satisfies parent diversity rule.
        if should_check('parent_diversity') and not self.check_parent_diversity(U):
            return False

        # 6. Check "growth" rule.
        if should_check('growth') and not self.check_growth(U):
            return False

        # 7. Coinshares are OK.
        # TODO: implementation missing

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
                        if W in set(units_list) and W not in units_added:
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
        if height < 0:
            return []

        result_list = []
        for U in self.max_units_per_process[process_id]:
            if U.height < height:
                continue
            while U is not None and U.height > height:
                U = U.self_predecessor
            if U.height == height:
                result_list.append(U)

        #remove possible duplicates in case process_id is a forker
        return list(set(result_list))

    def get_self_children(self, U):
        '''
        Returns the set of all units V in the poset such that V.self_predecessor == U
        NOTE: inefficient because units_by_height is inefficient.
        '''
        return self.units_by_height(U.creator_id, U.height + 1)









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


