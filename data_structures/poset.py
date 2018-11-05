'''This modul implements a poset - a core data structure.'''

import config

from unit import Unit


class Poset:
    '''This class is the core data structure of the Aleph protocol.'''

    def __init__(self, process_id, genesis_unit):
        '''
        :param int process_id: identification number of process whose local view is represented by this poset.
        :param unit genesis_unit: genesis unit shared by all processes
        '''
        self.process_id = process_id
        self.units = {genesis_unit.hash(): genesis_unit}
        self.signing_fct = config.SIGNING_FUNCTION

    def add_unit(self, unit):
        '''
        Adds a correct unit created, where correctness was checked by the sync
        thread if this unit was created by a different node or by
        the Poset.create_unit method.

        :param unit unit: unit to be added to the poset
        '''
        self.units[unit.hash()] = unit

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

    def level(self, unit):
        '''
        Calculates the level in the poset of the unit.
        '''

        pass

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

    def prime_units(self):
        '''
        Returns a set of all prime units.
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

    def less_then(self, U, V):
        '''
        Checks if U < V.
        '''

        pass

    def greater_than(self, U, V):
        '''
        Checks if U > V.
        '''

        pass

    def high_above(self, U, V):
        '''
        Check if U >> V.
        '''

        pass

    def high_below(self, U, V):
        '''
        Checks if U << V.
        '''

        pass

    def unit_by_height(self, process_id, height):
        '''
        Returns a unit or a list of units created by a given process of a given height.
        '''

        pass
