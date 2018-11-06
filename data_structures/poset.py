'''This modul implements a poset - a core data structure.'''

import config

from unit import Unit
from itertools import product


class Poset:
    '''This class is the core data structure of the Aleph protocol.'''

    def __init__(self, n_processes, process_id, genesis_unit):
        '''
        :param int n: the committee size
        :param int process_id: identification number of process whose local view is represented by this poset.
        :param unit genesis_unit: genesis unit shared by all processes
        '''
        self.n_processes = n_processes
        self.process_id = process_id
        self.genesis_unit = genesis_unit

        self.units = {genesis_unit.hash(): genesis_unit}
        self.max_units = [genesis_unit]
        self.max_units_per_process = [[] for _ in range(n_processes)]

        self.signing_fct = config.SIGNING_FUNCTION
        
        self.level_reached = 0
        self.prime_units_by_level = {0: [genesis_unit]}

    def add_unit(self, U):
        '''
        Adds a unit compliant with the rules, what was chacked by check_compliance.
        This method does the following:
            1. adds the unit U to the poset,
            2. sets U's self_parent, height, and floor fields,
            3. updates ceil field of predecessors of U,
            4. updates the lists of maximal elements in the poset.

        :param unit U: unit to be added to the poset
        '''

        # 1. add U to the poset
        self.units[U.hash()] = U

        # 2. set self_parent
        if self.max_units_per_process[self.process_id]:
            U.self_parent = self.units[self.max_units_per_process[self.process_id]]
        else:
            U.self_parent = None

        # 2. set height
        if U.self_parent:
            U.height = 0
        else:
            U.height = U.self_parent.height + 1

        # 2. set floor
        parents = [self.units[parent_hash] for parent_hash in U.parents]

        if parents[0] == self.genesis_unit:
            U.floor = [[] for _ in range(self.n_processes)]
        else:
            self.update_floor(U, parents)

        # 3. update ceil field of predecessors of U
        U.ceil = [[] for _ in range(self.n_processes)]
        parents = [self.units[parent_hash] for parent_hash in U.parents]
        for parent in parents:
            self.update_ceil(U, parent)

        # 4. update lists of maximal elements
        prev_max = U.self_parent
        if prev_max in self.max_units:
            self.max_units.remove(prev_max)

        self.max_units_per_process[self.process_id] = U
        self.max_units.append(U)

    def update_floor(self, U, parents):
        '''
        Updates floor of the unit U by merging and taking maximums of floors of parents.
        '''

        floor = parents[0].floor
        for parent, process_id in product(parents[1:], range(self.n_processes)):
            if not parent.floor[process_id]:
                continue
            if not floor[process_id]:
                floor[process_id] = parent.floor[process_id]
                continue

            if not self.forking_height[process_id] or self.forking_height[process_id] > U.height:
                if self.greater_than(parent.floor[process_id], floor[process_id]):
                    floor[process_id] = parent.floor[process_id]
                continue

            # list of elements in parent.floor[process_id] noncomparable with elements from floor[process_id]
            # this list is is added to floor
            forks = []
            for V in parent.floor[process_id]:
                # This flag checks if there is W comparable with V. If not then we add V to forks
                found_comparable, replace_index = False, None
                for k, W in enumerate(floor[process_id]):
                    if V.height > W.height:
                        if self.greater_than_within_process(V, W):
                            found_comparable = True
                            replace_index = k
                            break
                    if V.height < W.height:
                        if self.less_than_within_process(V, W):
                            found_comparable = True

                if not found_comparable:
                    forks.append(V)

                if replace is not None:
                    floor[process_id][replace_index] = V

            floor[process_id].extend(forks)

        U.floor = floor

    def update_ceil(self, U, V):
        '''
        Adds U to the ceil of V if the list is empty or if the process that created U
        produced forks that are not higher than U.
        After addition, it is called recursively for parents of V.
        '''

        if not V.ceil[U.creator_id] or (self.forking_height[U.creator_id] and
                                        self.forking_height[U.creator_id] <= U.height):
            V.ceil.append(U)
            parents = [self.units[parent_hash] for parent_hash in V.parents]
            for parent in parents:
                self.update_ceil(U, parent)

    def check_compliance(self, unit):
        '''
        Checks if unit follows the rules, i.e.:
            - parent diversity rule
            - anti-fork rules
            - has correct signature
            - its parents are in the Poset
            - is it prime
        '''

        pass


    def create_unit(self, txs):
        '''
        Creates a new unit and stores thx in it. Correctness of the txs is checked by a thread listening for new transactions.

        :param list txs: list of correct transactions
        '''

        # Pick parents for a new unit
        parents = [self.my_maximal()]
        for _ in range(config.N_PARENTS-1):  # TODO add a declaration of the number of parents a every unit
            parent = self.rand_maximal()
            if parent not in parents:
                parents.append(parent)

        # Create the new unit, the fields level, coinshares, and signature are filled next
        new_unit = Unit(self.process_id, parents, txs, [])

        # Calculate the level of the new unit
        new_unit.level = self.level(new_unit)

        # If the new unit is a prime unit, add coin shares to it
        if self.check_primeness(new_unit):
            new_unit.coinshares = self.choose_coinshares(parents)

        # All fields are filled, sign the unit
        new_unit.signature = self.sign(new_unit)

        # Add the new unit to the poset
        self.add_unit(new_unit)

    def sign(self, unit):
        '''
        Signs the unit.
        TODO This method should be probably a part of a process class which we don't have right now.
        '''

        pass

    def level(self, U):
        '''
        Calculates the level in the poset of the unit U.
        '''
        # TODO: so far this is a rather naive implementation -- loops over all prime units at level just below U
        
        if U is self.genesis_unit:
            return 0
        
        # let m be the max level of U's parents
        m = max([V.level for V in U.parents])
        # now, the level of U is either m or (m+1)
        # TODO: continue...
        
        # need to count all processes that produced a unit V of level m such that U'<<U
        # we can limit ourselves to prime units V
        processes_high_below = 0
        
        for V in self.get_prime_units_by_level(m):
            if self.high_below(V,U):
                processes_high_below += 1
        
        # same as (...)>=2/3*(...) but avoids floating point division
        if 3*processes_high_below>=2*N_processes:
            return m+1
        else:
            return m
            
        
        
        

        

    def choose_coinshares(self, unit):
        '''
        Implements threshold_coin algorithm from the paper.
        '''

        pass

    def check_primeness(self, unit):
        '''
        Check if the unit is prime.
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
        
    def get_prime_units_by_level(self, level):
        '''
        Returns the set of all prime units at a given level.
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
        
    def less_than_within_process(self, U, V, process_id):
        '''
        Checks if there exists a path (possibly U = V) from U to V going only through units created by process_id.
        Assumes that U.creator_id = V.creator_id = process_id
        '''
        if U.height > V.height:
            return False
        
        # if process_id is non-forking or at least U is below the process_id's forking level then clearly U has a path to V
        if (self.forking_level[process_id] is None) or U.height <= self.forking_level[process_id]:
            return True
            
        # at this point we know that this is a forking situation: we need go down the tree from V until we reach U's height
        # this will not take much time as process_id is banned for forking right after it is detected
        
        W = V
        while W.height > U.height:
            W = W.self_predecessor
        
        # TODO: make sure the below line does what it should
        return (W is U)
            

    def less_than(self, U, V):
        '''
        Checks if U <= V.
        '''
        proc_U = U.creator_id
        proc_V = V.creator_id
        
        for W in V.floor[proc_U]:
            if self.less_than_within_process(U, V, proc_U):
                return True
                
        return False

    def greater_than(self, U, V):
        '''
        Checks if U >= V.
        '''
        return less_than(V,U)


    def high_above(self, U, V):
        '''
        Checks if U >> V.
        '''
        return high_below(V,U)


    def high_below(self, U, V):
        '''
        Checks if U << V.
        '''
        processes_in_support = 0
        
        for process_id in range(self.N_processes):
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
                    if self.less_than_within_process(U_ceil, V_floor, process_id):
                        in_support = True
                        break

            if in_support:
                processes_in_support += 1
        
        
        # same as processes_in_support>=2/3 N_procesees but avoids floating point division
        return 3*processes_in_support>=2*N_processes
           

    def unit_by_height(self, process_id, height):
        '''
        Returns a unit or a list of units created by a given process of a given height.
        '''

        pass
