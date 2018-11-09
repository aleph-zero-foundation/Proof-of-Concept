'''This module implements a poset - a core data structure.'''

from itertools import product

from unit import Unit
import config


class Poset:
    '''This class is the core data structure of the Aleph protocol.'''

    def __init__(self, n_processes, process_id, genesis_unit):
        '''
        :param int n_processes: the committee size
        :param int process_id: identification number of process whose local view is represented by this poset.
        :param unit genesis_unit: genesis unit shared by all processes
        '''
        self.n_processes = n_processes
        self.process_id = process_id
        self.genesis_unit = genesis_unit

        self.units = {genesis_unit.hash(): genesis_unit}
        self.max_units = [genesis_unit]
        self.max_units_per_process = [[] for _ in range(n_processes)]
        self.forking_height = [None for _ in range(n_processes)]

        self.signing_fct = config.SIGNING_FUNCTION

        self.level_reached = 0
        self.prime_units_by_level = {0: [genesis_unit]}
        
        # For every unit U maintain a list of processes that can be proved forking by looking at the lower-cone of U
        self.known_forkers_by_unit = {genesis_unit.hash(): []}

    def add_unit(self, U):
        '''
        Adds a unit compliant with the rules, what was chacked by check_compliance.
        This method does the following:
            1. adds the unit U to the poset,
            2. sets U's self_predecessor, height, and floor fields,
            3. updates ceil field of predecessors of U,
            4. updates the lists of maximal elements in the poset.
            5. adds an entry to known_forkers_by_unit

        :param unit U: unit to be added to the poset
        '''

        # 1. add U to the poset
        self.units[U.hash()] = U

        # 2. set self_predecessor
        if self.max_units_per_process[self.process_id]:
            U.self_predecessor = self.units[self.max_units_per_process[self.process_id]]
        else:
            U.self_predecessor = None

        # 2. set height
        if U.self_predecessor is None:
            U.height = 0
        else:
            U.height = U.self_predecessor.height + 1

        # 2. set floor
        parents = [self.units[parent_hash] for parent_hash in U.parents]
        
        if len(parents)==1 and parents[0] is self.genesis_unit:
            U.floor = [[] for _ in range(self.n_processes)]
        else:
            self.update_floor(U, parents)

        # 3. update ceil field of predecessors of U
        U.ceil = [[] for _ in range(self.n_processes)]
        parents = [self.units[parent_hash] for parent_hash in U.parents]
        for parent in parents:
            self.update_ceil(U, parent)

        # 4. update lists of maximal elements
        prev_max = U.self_predecessor
        if prev_max in self.max_units:
            self.max_units.remove(prev_max)

        self.max_units_per_process[self.process_id] = U
        self.max_units.append(U)
        
        
        # 5. add an entry to known_forkers_by_unit
        forkers = []
        # process_id is known forking if U.floor[process_id] has more than one element
        for process_id in range(self.n_processes):
            if len(U.floor[process_id]):
                forkers.append(process_id)
        
        self.known_forkers_by_unit[U.hash()] = forkers
                

    def update_floor(self, U, parents):
        '''
        Updates floor of the unit U by merging and taking maximums of floors of parents.
        '''
        floor = [[] for _ in range(self.n_processes)]
        # the floor of U w.r.t. to its creator process is just [U]
        floor[U.creator_id] = [U]
        for parent, process_id in product(parents, range(self.n_processes)):
            # list of elements in parent.floor[process_id] noncomparable with elements from floor[process_id]
            # this list is then added to floor
            forks = []
            for V in parent.floor[process_id]:
                # This flag checks if there is W comparable with V. If not then we add V to forks
                found_comparable, replace_index = False, None
                for k, W in enumerate(floor[process_id]):
                    if V.height > W.height and self.greater_than_or_equal_within_process(V, W):
                        found_comparable = True
                        replace_index = k
                        break
                    if V.height <= W.height and self.less_than_or_equal_within_process(V, W):
                        found_comparable = True
                        break

                if not found_comparable:
                    forks.append(V)

                if replace_index is not None:
                    floor[process_id][replace_index] = V

            floor[process_id].extend(forks)

        U.floor = floor
        
    def find_forking_evidence(self, parents, process_id):
        '''
        Checks whether the list of units (parents) contains evidence that process_id is forking.
        :param list parents: list of units to be checked for evidence of process_id forking
        :param int process_id: the identification number of process to be verified
        :returns: Boolean value, True if forking evidence is present, False otherwise.
        '''
        known_process_maximal_units = []
        
        for V in parents:
            if len(V.floor[process_id]) > 1:
                return True
            known_process_maximal_units.extend(V.floor[process_id])
        
        # Check if all pairs of maximal process_id's units are comparable within process_id
        for V in known_process_maximal_units:
            for W in known_process_maximal_units:
                if not (self.greater_than_or_equal_within_process(V, W) or self.greater_than_or_equal_within_process(W, V)):
                    # we have found a pair (V,W) of incomparable units created by process_id
                    return True
                    
        return False


    def update_ceil(self, U, V):
        '''
        Adds U to the ceil of V if the list is empty or if the process that created U
        produced forks that are not higher than U.
        After addition, it is called recursively for parents of V.
        '''
        
        # TODO: at some point we should change it to a version with an explicit stack
        # TODO: Python has some strange recursion depth limits
        if not V.ceil[U.creator_id] or (self.forking_height[U.creator_id] and
                                        self.forking_height[U.creator_id] <= U.height):
            V.ceil.append(U)
            parents = [self.units[parent_hash] for parent_hash in V.parents]
            for parent in parents:
                self.update_ceil(U, parent)

                
    def get_known_forkers(self, U):
        '''
        Finds all processes that can be proved forking given evidence in the lower-cone of unit U.
        :param unit U: unit whose lower-cone should be considered for forking attempts
        '''
        
        # TODO: the implementation of this function might be important for efficiency as a whole
        # TODO: the current version maintains a data structure that makes this function fast and easy to implement
        # TODO: make sure this data structure is properly updated when new units are added!
        # NOTE: this implementation assumes that U has been already added to the poset (data structure has been updated)
        
        assert (U.hash() in self.known_forkers_by_unit.keys()), "Unit U has not yet been added to known_forkers_by_unit"
        
        return self.known_forkers_by_unit[U.hash()]


    
    def check_compliance(self, U):
        '''
        Checks if the unit U is correct and follows the rules of creating units, i.e.:
            1. Parents of U are in the poset.
            2. Has correct signature.
            3. Satisfies anti-fork policy.
            4. Satisfies parent diversity rule.
            5. Check "grow" rule.
            6. The coinshares are OK, i.e., U contains exactly the coinshares it is supposed to contain.
        :param unit U: unit whose compliance is being tested
        '''
        # TODO: there might have been other compliance rules that have been forgotten...
        
        # 1. Parents of U are in the poset.
        if not self.check_parents_exist(U):
            return False
            
        # 2. Has correct signature.
        if not self.check_signature_correct(U):
            return False
            
        # 3. Satisfies anti-fork policy.    
        if not self.check_anti_fork(U):
            return False
            
        # 4. Satisfies parent diversity rule.
        if not self.check_parent_diversity(U):
            return False
            
        # 5. Check "grow" rule.
        # TODO: implementation missing
            
        # 6. Coinshares are OK.
        # TODO: implementation missing
            
        return True
        
    def check_anti_fork(self, U):
        '''
        Checks if the unit U respects the anti-forking policy, i.e.:
        The following situations A), B) are not allowed:
        A)
            - There exists a process j, s.t. (one of U's parents was created by j) OR (U was created by j)
            AND
            - U has as one of the parents a unit that has evidence that j is forking.
        B) 
            - The creator of U is the process j
            AND
            - The parents of U (combined) have evidence that j is forking
            
        :param unit U: unit that is checked for respecting anti-forking policy
        :returns: Boolean value, True if U respects the policy, False otherwise.
        '''
        # Check for situation A)
        forkers_known_by_parents = []
        for V_hash in U.parents:
            forkers_known_by_parents.extend(self.known_forkers_by_unit[V_hash])
        
        if U.creator_id in forkers_known_by_parents:
            return False
            
        for V_hash in U.parents:
            V = self.units[V_hash]
            if V.creator_id in forkers_known_by_parents:
                return False
        
        # Check for situation B)
        if find_forking_evidence([self.units[V_hash] for V_hash in U.parents], U.creator_id):
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
        
    def check_parents_exist(self, U):
        '''
        Checks if unit U's parents already exist in the poset
        :param unit U: unit whose parents are being checked
        '''
        
        for V_hash in U.parents:
            if V_hash not in self.units.keys():
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
        # TODO: make sure U.self_predecessor is correctly set when invoking this method
        # Special case: U's only parent is the genesis_unit
        if len(U.parents)==1 and self.units[U.parents[0]] is self.genesis_unit:
            return True
            
        proposed_parent_processes = [self.units[V_hash].creator_id for V_hash in U.parents]
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
            # W's only parent is the genesis unit -> STOP
            if len(W.parents)==1 and self.units[W.parents[0]] is self.genesis_unit:
                break
            # flag for whether at the current level there is any occurence of a parent process proposed by U
            proposed_parent_process_occurence = False
            
            for V_hash in W.parents:
                V = self.units[V_hash]
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

    def create_unit(self, txs):
        '''
        Creates a new unit and stores thx in it. Correctness of the txs is checked by a thread listening for new transactions.

        :param list txs: list of correct transactions
        '''
        
        # NOTE: perhaps we (as an honest process) should always try to create a unit that 
        # NOTE: gives evidence of another process forking

        pass

    def sign(self, unit):
        '''
        Signs the unit.
        TODO This method should be probably a part of a process class which we don't have right now.
        '''

        pass

    def level(self, U):
        '''
        Calculates the level in the poset of the unit U.
        :param unit U: the unit whose level is being requested
        '''
        # TODO: so far this is a rather naive implementation -- loops over all prime units at level just below U

        if U is self.genesis_unit:
            return 0
            
        if U.level is not None:
            return U.level

        # let m be the max level of U's parents
        parents = [self.units[parent_hash] for parent_hash in U.parents]
        m = max([V.level for V in parents])
        # now, the level of U is either m or (m+1)

        # need to count all processes that produced a unit V of level m such that U'<<U
        # we can limit ourselves to prime units V
        processes_high_below = 0

        for V in self.get_prime_units_by_level(m):
            if self.high_below(V, U):
                processes_high_below += 1

        # same as (...)>=2/3*(...) but avoids floating point division
        if 3*processes_high_below >= 2*self.n_processes:
            return m+1
        else:
            return m

    def choose_coinshares(self, unit):
        '''
        Implements threshold_coin algorithm from the paper.
        '''

        pass

    def check_primeness(self, U):
        '''
        Check if the unit is prime.
        :param unit U: the unit to be checked for being prime
        '''
        if (U is self.genesis_unit):
            return True
        
        # U is prime iff its self_predecessor level is strictly smaller
        return self.level(U) > self.level(U.self_predecessor)
        

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

    def get_prime_units_by_level(self, level):
        '''
        Returns the set of all prime units at a given level.
        :param int level: the requested level of units
        '''
        # TODO: this is a naive implementation
        # TODO: make sure that at creation of a prime unit it is added to the dict self.prime_units_by_level
        return self.prime_units_by_level[level]

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

    def diff(self, other):
        '''
        Returns a set of units that are in this poset and that are not in the other poset.
        '''

        pass

    def less_than_or_equal_within_process(self, U, V):
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
        if (self.forking_height[process_id] is None) or U.height <= self.forking_height[process_id]:
            return True

        # at this point we know that this is a forking situation: we need go down the tree from V until we reach U's height
        # this will not take much time as process_id is banned for forking right after it is detected

        W = V
        while W.height > U.height:
            W = W.self_predecessor

        # TODO: make sure the below line does what it should
        return (W is U)
        
    def greater_than_or_equal_within_process(self, U, V):
        '''
        Checks if there exists a path (possibly U = V) from V to U going only through units created by their creator process.
        Assumes that U.creator_id = V.creator_id = process_id
        :param unit U: first unit to be tested
        :param unit V: second unit to be tested
        '''
        return less_than_or_equal_within_process(self, V, U):


    def less_than_or_equal(self, U, V):
        '''
        Checks if U <= V.
        :param unit U: first unit to be tested
        :param unit V: second unit to be tested
        '''
        proc_U = U.creator_id
        proc_V = V.creator_id

        for W in V.floor[proc_U]:
            if self.less_than_or_equal_within_process(U, V, proc_U):
                return True

        return False

    def greater_than_or_equal(self, U, V):
        '''
        Checks if U >= V.
        :param unit U: first unit to be tested
        :param unit V: second unit to be tested
        '''
        return self.less_than_or_equal(V, U)

    def high_above(self, U, V):
        '''
        Checks if U >> V.
        :param unit U: first unit to be tested
        :param unit V: second unit to be tested
        '''
        return self.high_below(V, U)


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
                    if self.less_than_or_equal_within_process(U_ceil, V_floor, process_id):
                        in_support = True
                        break

            if in_support:
                processes_in_support += 1

        # same as processes_in_support>=2/3 n_procesees but avoids floating point division
        return 3*processes_in_support >= 2*self.n_processes


    def unit_by_height(self, process_id, height):
        '''
        Returns a unit or a list of units created by a given process of a given height.
        '''

        pass
