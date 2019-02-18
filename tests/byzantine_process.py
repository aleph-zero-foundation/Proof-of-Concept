import logging

from aleph.data_structures import Unit
from aleph.process import Process
import aleph.const as consts


class ByzantineProcess(Process):

    def __init__(self,
                 n_processes,
                 process_id,
                 secret_key,
                 public_key,
                 address_list,
                 public_key_list,
                 tx_receiver_address,
                 userDB=None,
                 validation_method='LINEAR_ORDERING',
                 gossip_strategy='unif_random',
                 level_limit=2):

        Process.__init__(self,
                         n_processes,
                         process_id,
                         secret_key,
                         public_key,
                         address_list,
                         public_key_list,
                         tx_receiver_address,
                         userDB,
                         validation_method,
                         gossip_strategy=gossip_strategy)
        self._logger = logging.getLogger(consts.LOGGER_NAME)
        self._first_forking_unit = None
        self._second_forking_unit = None
        # level after which this instance should create some forking unit
        self._level_limit = level_limit
        # current level of the poset
        self._level = 0
        # maximal number of tries of invocations of the create_unit method
        self._create_limit = 100
        # used by byzantine_linear_ordering.py
        self.forking_units = []

    def _is_byzantine_criteria_satisfied(self, unit):
        logger = self._logger
        logger.debug(f"checking byzantine node's criteria for level {self._level}.")
        if unit.creator_id != self.process_id:
            logger.debug(f'unit was not created by this process')
            return False
        if self._first_forking_unit is not None:
            logger.debug(f'forking node already created - called at level ({self._level})')
            return False
        if self._level < self._level_limit:
            logger.debug(f'byzantine criteria: level is to small ({self._level})')
            return False
        logger.debug(f'byzantine criteria met for level {self._level}')
        return True

    def disable(self):
        # disable the dispatch_syncs task
        self._keep_syncing = False
        self._keep_adding = False

    def _handle_byzantine_state(self, unit, forking_unit):
        '''
        Method executed after a forking unit was created. It allows derived types to override the default behavior.
        '''
        self._first_forking_unit = unit
        self._second_forking_unit = forking_unit
        self.forking_units.append(forking_unit)

    def _add_byzantine_unit(self, process, unit):
        # NOTE: if you create two units and then change the first parent of one of them, then they can be on different levels
        # and that change might make the value of the level attribute inadequate

        logger = self._logger
        logger.debug('adding forking units')

        # double spend all transactions from the previously created unit
        forking_unit = process.create_unit(unit.transactions(), prefer_maximal=consts.USE_MAX_PARENTS)
        if forking_unit is None:
            logger.debug("can't create a forking unit")
            return None
        logger.debug('created a forking unit')
        forking_unit.parents[0] = unit.parents[0]
        forking_unit.height = unit.height
        process.poset.prepare_unit(forking_unit)
        if not process.poset.check_compliance(forking_unit):
            return None
        process.sign_unit(forking_unit)
        if not Process.add_unit_to_poset(process, forking_unit):
            logger.debug("can't add a forking unit to the poset")
            return None

        self._handle_byzantine_state(unit, forking_unit)
        return forking_unit

    def _log_new_unit(self, new_unit, first_forking_unit, second_forking_unit, poset):
        logger = self._logger
        if not self._check_for_self_diamond(new_unit, first_forking_unit, second_forking_unit, poset):
            logger.debug(
                'newly created unit is above the both forking units')
            logger.debug(f'''diamond unit creator: {new_unit.creator_id}
                         first parent: {self.poset.units[new_unit.parents[1]]}''')
            logger.debug(f'diamond unit parents: {new_unit.parents}')
        else:
            if first_forking_unit is not None:
                above_first = poset.below(first_forking_unit, new_unit)
                if above_first:
                    logger.debug(
                        'newly created unit is above the first forking unit')

            if second_forking_unit is not None:
                above_second = poset.below(second_forking_unit, new_unit)
                if above_second:
                    logger.debug(
                        'newly created unit is above the second forking unit')

    def _update_state(self, poset, new_unit):
        if self._first_forking_unit is not None:
            return
        self._level = max(self._level, poset.level(new_unit))

    def add_unit_to_poset(self, U):
        logger = self._logger
        logger.debug(f'called at level {self._level}')
        already_in_poset = U.hash() in self.poset.units.keys()
        if not Process.add_unit_to_poset(self, U):
            logger.debug("can't add a unit to the poset")
            return False
        if already_in_poset:
            return True
        self._update_state(self.poset, U)
        if self._is_byzantine_criteria_satisfied(U):
            if self._add_byzantine_unit(self, U) is None:
                logger.debug('failed to add a forking unit')
                return False
            else:
                logger.debug('successfully added a forking unit')
                return True
        else:
            self._log_new_unit(U, self._first_forking_unit, self._second_forking_unit, self.poset)
            logger.debug('added a new unit to the poset')
            return True

    def _check_for_self_diamond(self, U, first_forking_unit, second_forking_unit, poset):
        if first_forking_unit is None or second_forking_unit is None:
            return True
        if (
                poset.below(first_forking_unit, U) and
                poset.below(second_forking_unit, U)
        ):
            return False
        return True

    def create_unit(self, txs, prefer_maximal=consts.USE_MAX_PARENTS):
        '''
        Tries to creates a "correct" unit.
        '''
        counter = 0
        while counter < self._create_limit:
            counter += 1
            try:
                U = Process.create_unit(self, txs, prefer_maximal)
                if U is None:
                    self._logger.debug('"Process.create_unit" returned None')
                    continue
                self.poset.prepare_unit(U)
                if (
                        self.poset.check_compliance(U) and
                        self._check_for_self_diamond(U, self._first_forking_unit, self._second_forking_unit, self.poset)
                ):
                    return U
            except Exception as ex:
                self._logger.debug(ex)

        return None

    @staticmethod
    def translate_unit(U, process):
        '''
        Takes a unit from the poset of a process A and the mapping hashes->Units for the poset for process B. Returns a new unit
        (with correct references to corresponding parent units in B) to be added to B's poset. The new unit has all the data in
        the floor/ceil/level/... fields erased.
        '''
        parent_hashes = [V.hash() for V in U.parents]
        parents = [process.poset.units[V] for V in parent_hashes]
        U_new = Unit(U.creator_id, parents, U.transactions(), U.coin_shares)
        process.sign_unit(U_new)
        return U_new
