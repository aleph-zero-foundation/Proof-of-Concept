import asyncio
import concurrent
import logging
import multiprocessing
import random
import psutil
import os
import time


from aleph.data_structures import Poset, UserDB
from aleph.crypto import CommonRandomPermutation
from aleph.network import listener, sync, tx_listener
from aleph.config import CREATE_FREQ, SYNC_INIT_FREQ, LOGGER_NAME
from aleph.utils import timer



class Process:
    '''This class is the main component of the Aleph protocol.'''


    def __init__(self, n_processes, process_id, secret_key, public_key, address_list, public_key_list, tx_receiver_address,
                userDB=None, validation_method='SNAP', enable_tcoin=False, profile_add_unit=True):
        '''
        :param int n_processes: the committee size
        :param int process_id: the id of the current process
        :param string secret_key: the private key of the current process
        :param list addresses: the list of length n_processes containing addresses (host, port) of all committee members
        :param list public_keys: the list of public keys of all committee members
        :param string validation_method: the method of validating transactions/units: either "SNAP" or "LINEAR_ORDERING" or None for no validation
        '''

        self.n_processes = n_processes
        self.process_id = process_id
        self.validation_method = validation_method
        self.enable_tcoin = enable_tcoin

        self.secret_key = secret_key
        self.public_key = public_key

        self.public_key_list = public_key_list
        self.address_list = address_list
        self.host = address_list[process_id][0]
        self.port = address_list[process_id][1]

        self.tx_receiver_address = tx_receiver_address
        self.prepared_txs = []

        self.crp = CommonRandomPermutation([pk.to_hex() for pk in public_key_list])
        self.poset = Poset(self.n_processes, self.crp, use_tcoin = enable_tcoin, process_id = self.process_id)
        self.userDB = userDB
        if self.userDB is None:
            self.userDB = UserDB()

        # dictionary {user_public_key -> set of (tx, U)}  where tx is a pending transaction (sent by this user) in unit U
        self.pending_txs = {}

        # a bitmap specifying for every process whether he has been detected forking
        self.is_forker = [False for _ in range(self.n_processes)]

        self.syncing_tasks = []

        self.validated_transactions = []

        # hashes of units that have not yet been linearly ordered
        self.unordered_units = set()

        # hashes of units in linear order
        self.linear_order = []

        # we number all the syncs performed by process with unique ids (both outcoming and incoming)
        self.sync_id = 0

        # initialize logger
        self.logger = logging.getLogger(LOGGER_NAME)
        self.profile_add_unit = profile_add_unit
        self.add_unit_runtimes = []


    def sign_unit(self, U):
        '''
        Signs the unit.
        :param unit U: the unit to be signed.
        '''
        U.signature = self.secret_key.sign(U.bytestring())


    def process_txs_in_unit_list(self, list_U):
        '''
        For now this just counts unique transactions in all the units in list_U.
        :returns: The number of unique transactions
        '''
        all_txs = set()
        for U in list_U:
            for tx in U.transactions():
                all_txs.add(tx)

        return len(all_txs)



    def add_unit_and_snap_validate(self, U):
        '''
        Add a (compliant) unit to the poset and attempt fast validation of transactions inside.
        '''
        list_of_validated_units = []
        self.poset.add_unit(U, list_of_validated_units)
        self.logger.info(f'add_unit_to_poset {self.process_id} -> Validated a set of {len(list_of_validated_units)} units.')
        for tx in U.transactions():
            if tx.issuer not in self.pending_txs.keys():
                self.pending_txs[tx.issuer] = set()
            self.pending_txs[tx.issuer].add((tx,U))
        for V in list_of_validated_units:
            if V.transactions():
                newly_validated = self.validate_transactions_in_unit(V, U)
                self.logger.info(f'add_unit_to_poset {self.process_id} -> Validated a set of {len(newly_validated)} transactions.')
                self.validated_transactions.extend(newly_validated)


    def add_unit_and_extend_linear_order(self, U):
        '''
        Add a (compliant) unit to the poset, try to find a new timing unit and if succeded, extend the linear order.
        '''
        self.poset.add_unit(U)
        self.unordered_units.add(U.hash())
        if self.poset.is_prime(U):
            new_timing_units = self.poset.attempt_timing_decision()
            self.logger.info(f'prime_unit {self.process_id} | New prime unit at level {U.level} : {U.short_name()}')
            for U_timing in new_timing_units:
                self.logger.info(f'timing_new {self.process_id} | Timing unit at level {U_timing.level} established.')
            for U_timing in new_timing_units:
                with timer(self.process_id, 'linear_order'):
                    units_to_order = []
                    for V_hash in self.unordered_units:
                        V = self.poset.unit_by_hash(V_hash)
                        if self.poset.below(V, U_timing):
                            units_to_order.append(V)
                    ordered_units = self.poset.break_ties(units_to_order)
                    ordered_units_hashes = [W.hash() for W in ordered_units]
                    self.linear_order += ordered_units_hashes
                    self.unordered_units = self.unordered_units.difference(ordered_units_hashes)

                    printable_unit_hashes = ''.join([' '+W.short_name() for W in ordered_units])
                    n_txs = self.process_txs_in_unit_list(ordered_units)

                self.logger.info(f'add_linear_order {self.process_id} | At lvl {U_timing.level} added {len(units_to_order)} units and {n_txs} txs to the linear order {printable_unit_hashes}')
                timer.write_summary(where=self.logger, groups=[self.process_id])
                timer.reset(self.process_id)


    def add_unit_to_poset(self, U):
        '''
        Checks compliance of the unit U and adds it to the poset (unless already in the poset). Subsequently validates transactions using U.
        :param unit U: the unit to be added
        :returns: boolean value: True if succesfully added, False if unit is not compliant
        '''
        if self.profile_add_unit:
            start_time = time.time()

        if U.hash() in self.poset.units.keys():
            return True

        self.poset.prepare_unit(U)
        if self.poset.check_compliance(U):
            old_level = self.poset.level_reached

            if self.validation_method == 'SNAP':
                self.add_unit_and_snap_validate(U)
            elif self.validation_method == 'LINEAR_ORDERING':
                self.add_unit_and_extend_linear_order(U)
            else:
                self.poset.add_unit(U)

            if self.poset.level_reached > old_level:
                self.logger.info(f"new_level {self.process_id} | Level {self.poset.level_reached} reached")

        else:
            return False

        if self.profile_add_unit:
            tot_time = time.time() - start_time
            self.add_unit_runtimes.append(tot_time)

        return True


    def validate_transactions_in_unit(self, U, U_validator):
        '''
        Returns a list of transactions in U that can be fast-validated if U's validator unit is U_validator
        :param unit U: unit whose transactions should be validated
        :param unit U_validator: a unit that validates U (is high above U)
        :returns: list of all transactions in unit U that can be fast-validated
        '''
        validated_transactions = []
        for tx in U.transactions():
            user_public_key = tx.issuer

            assert user_public_key in self.pending_txs.keys(), f"No transaction is pending for user {user_public_key}."
            assert (tx, U) in self.pending_txs[user_public_key], "Transaction not found among pending"
            if tx.index != self.userDB.last_transaction(tx.issuer) + 1:
                self.logger.info(f'tx_validation {self.process_id} | transaction failed to validate because its index is {tx.index}, while the previous one was {self.userDB.last_transaction(tx.issuer)}')
                continue
            transaction_fork_present = False
            for (pending_txs, V) in self.pending_txs[user_public_key]:
                if tx.index == pending_txs.index:
                    if (tx, U) != (pending_txs, V):
                        if self.poset.below(V, U_validator):
                            transaction_fork_present = True
                            break

            if not transaction_fork_present:
                if self.userDB.check_transaction_correctness(tx):
                    self.userDB.apply_transaction(tx)
                    validated_transactions.append(tx)


        for tx in validated_transactions:
            self.pending_txs[tx.issuer].discard((tx,U))
        return validated_transactions


    async def create_add(self, txs_queue, serverStarted):
        await serverStarted.wait()
        #while True:
        for _ in range(80):
            # log current memory consumption
            memory_usage_in_mib = (psutil.Process(os.getpid()).memory_info().rss)/(2**20)
            self.logger.info(f'memory_usage {self.process_id} | {memory_usage_in_mib:.4f} MiB')

            # log running time of add_unit, (>10 so that we do not pollute logs too much)
            if len(self.add_unit_runtimes) > 10:
                tot_time_of_add_unit = sum(self.add_unit_runtimes)
                n_units_added = len(self.add_unit_runtimes)
                self.logger.info(f'add_run_time {self.process_id} | Added {n_units_added} in {tot_time_of_add_unit:.4f} sec')
                # empty the list
                self.add_unit_runtimes = []

            txs = self.prepared_txs
            with timer(self.process_id, 'create_unit'):
                new_unit = self.poset.create_unit(self.process_id, txs, strategy = "link_self_predecessor", num_parents = 2)
            timer.write_summary(where=self.logger, groups=[self.process_id])
            timer.reset(self.process_id)

            if new_unit is not None:
                self.poset.prepare_unit(new_unit)
                assert self.poset.check_compliance(new_unit), "A unit created by our process is not passing the compliance test!"
                self.sign_unit(new_unit)
                self.logger.info(f"create_add {self.process_id} | Created a new unit {new_unit.short_name()}")
                #self.poset.add_unit(new_unit)
                self.add_unit_to_poset(new_unit)

                if not txs_queue.empty():
                    self.prepared_txs = txs_queue.get()
                else:
                    self.prepared_txs = []

            await asyncio.sleep(CREATE_FREQ)


    async def keep_syncing(self, executor, serverStarted):
        await serverStarted.wait()
        #while True:
        for _ in range(80):
            sync_candidates = list(range(self.n_processes))
            sync_candidates.remove(self.process_id)
            target_id = random.choice(sync_candidates)
            self.syncing_tasks.append(asyncio.create_task(sync(self, self.process_id, target_id, self.address_list[target_id], self.public_key_list, executor)))

            await asyncio.sleep(SYNC_INIT_FREQ)


    async def run(self):
        # start another process listening for incoming txs
        self.logger.info(f'start_process {self.process_id} | Starting a new process in committee of size {self.n_processes}')
        txs_queue = multiprocessing.Queue()
        p = multiprocessing.Process(target=tx_listener, args=(self.tx_receiver_address, txs_queue))
        p.start()

        serverStarted = asyncio.Event()
        executor = None
        creator_task = asyncio.create_task(self.create_add(txs_queue, serverStarted))
        listener_task = asyncio.create_task(listener(self, self.process_id, self.address_list, self.public_key_list, executor, serverStarted))
        syncing_task = asyncio.create_task(self.keep_syncing(executor, serverStarted))

        await asyncio.gather(syncing_task, creator_task)
        self.logger.info(f'listener_done {self.process_id} | Gathered results; cancelling listener')
        listener_task.cancel()

        p.kill()
