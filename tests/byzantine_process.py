import logging

from aleph.const import LOGGER_NAME, USE_MAX_PARENTS
from aleph.process import Process


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
                 validation_method='SNAP',
                 gossip_strategy='unif_random',
                 level_limit=3):

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
        self.logger = logging.getLogger(LOGGER_NAME)
        self.memo_1 = None
        self.memo_2 = None
        self.level_limit = level_limit
        self.level = 0
        self.create_limit = 100
        self.forking_units = []

    def is_byzantine_criteria_satisfied(self, unit):
        logger = self.logger
        logger.debug(f"checking byzantine node's criteria for level {self.level}.")
        if unit.creator_id != self.process_id:
            logger.debug(f'unit was not created by this process')
            return False
        if self.memo_1 is not None:
            logger.debug(f'forking node already created - called at level ({self.level})')
            return False
        if self.level < self.level_limit:
            logger.debug(f'byzantine criteria: level is to small ({self.level})')
            return False
        logger.debug(f'byzantine criteria met for level {self.level}')
        return True

    def disable(self):
        # disable the dispatch_syncs task
        self.keep_syncing = False
        self.keep_adding = False

    def handle_byzantine_state(self, unit, forking_unit):
        self.memo_1 = unit
        self.memo_2 = forking_unit
        self.forking_units.append(forking_unit)

    def add_byzantine_unit(self, process, unit):
        # NOTE: if you create two units and then change the first parent of one of them, then they can be on different levels
        # and that change might make the value of the level attribute inadequate

        logger = self.logger
        logger.debug('adding forking units')

        # double spend all transactions from the previously created unit
        forking_unit = process.create_unit(unit.transactions(), prefer_maximal = USE_MAX_PARENTS)
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

        self.handle_byzantine_state(unit, forking_unit)
        return forking_unit

    def log_new_unit(self, new_unit, memo_1, memo_2, poset):
        logger = self.logger
        if not self.check_for_self_diamond(new_unit, memo_1, memo_2, poset):
            logger.debug(
                'newly created unit is above the both forking units')
            logger.debug(f'''diamond unit creator: {new_unit.creator_id}
                         first parent: {self.poset.units[new_unit.parents[1]]}''')
            logger.debug(f'diamond unit parents: {new_unit.parents}')
        else:
            if memo_1 is not None:
                above_first = poset.below(memo_1, new_unit)
                if above_first:
                    logger.debug(
                        'newly created unit is above the first forking unit')

            if memo_2 is not None:
                above_second = poset.below(memo_2, new_unit)
                if above_second:
                    logger.debug(
                        'newly created unit is above the second forking unit')

    def update_state(self, poset, new_unit):
        if self.memo_1 is not None:
            return
        self.level = max(self.level, poset.level(new_unit))

    def add_unit_to_poset(self, U):
        logger = self.logger
        logger.debug(f'called at level {self.level}')
        already_in_poset = U.hash() in self.poset.units.keys()
        if not Process.add_unit_to_poset(self, U):
            logger.debug("can't add a unit to the poset")
            return False
        if already_in_poset:
            return True
        self.update_state(self.poset, U)
        if self.is_byzantine_criteria_satisfied(U):
            if self.add_byzantine_unit(self, U) is None:
                logger.debug('failed to add a forking unit')
                return False
            else:
                logger.debug('successfully added a forking unit')
                return True
        else:
            self.log_new_unit(U, self.memo_1, self.memo_2, self.poset)
            logger.debug('added a new unit to the poset')
            return True

    def check_for_self_diamond(self, U, memo_1, memo_2, poset):
        if memo_1 is None or memo_2 is None:
            return True
        if (
                poset.below(memo_1, U) and
                poset.below(memo_2, U)
        ):
            return False
        return True

    def create_unit(self, txs, prefer_maximal = USE_MAX_PARENTS):
        counter = 0
        while counter < self.create_limit:
            counter += 1
            try:
                U = Process.create_unit(self, txs, prefer_maximal)
                if U is None:
                    continue
                self.poset.prepare_unit(U)
                if (
                        self.poset.check_compliance(U) and
                        self.check_for_self_diamond(U, self.memo_1, self.memo_2, self.poset)
                ):
                    return U
            except Exception as ex:
                self.logger.debug(ex)

        return None
