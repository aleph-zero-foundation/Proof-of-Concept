import asyncio
import concurrent
import logging
import multiprocessing
import random

from aleph.data_structures.poset import Poset
from aleph.data_structures.userDB import UserDB
from aleph.network import listener, sync, tx_listener
from aleph.config import CREATE_FREQ, SYNC_INIT_FREQ, LOGGER_NAME


class Process:
    '''This class is the main component of the Aleph protocol.'''


    def __init__(self, n_processes, process_id, secret_key, public_key, address_list, public_key_list, tx_receiver_address):
        '''
        :param int n_processes: the committee size
        :param int process_id: the id of the current process
        :param string secret_key: the private key of the current process
        :param list addresses: the list of length n_processes containing addresses (host, port) of all committee members
        :param list public_keys: the list of public keys of all committee members
        '''

        self.n_processes = n_processes
        self.process_id = process_id

        self.secret_key = secret_key
        self.public_key = public_key

        self.public_key_list = public_key_list
        self.address_list = address_list
        self.host = address_list[process_id][0]
        self.port = address_list[process_id][1]

        self.tx_receiver_address = tx_receiver_address
        self.prepared_txs = []

        self.poset = Poset(self.n_processes)
        self.userDB = UserDB()

        # dictionary {user_public_key -> set of (tx, U)}  where tx is a pending transaction (sent by this user) in unit U
        self.pending_txs = {}

        # a bitmap specifying for every process whether he has been detected forking
        self.is_forker = [False for _ in range(self.n_processes)]

        self.syncing_tasks = []

        self.validated_transactions = []

    def sign_unit(self, U):
        '''
        Signs the unit.
        :param unit U: the unit to be signed.
        '''

        message = U.to_message()
        U.signature = self.secret_key.sign(message)

    def add_unit_to_poset(self, U):
        '''
        Checks compliance of the unit U and adds it to the poset (unless already in the poset). Subsequently validates transactions using U.
        :param unit U: the unit to be added
        :returns: boolean value: True if succesfully added, False if unit is not compliant
        '''
        logger = logging.getLogger(LOGGER_NAME)
        if U.hash() in self.poset.units.keys():
            return True

        if self.poset.check_compliance(U):
            list_of_validated_units = []
            self.poset.add_unit(U, list_of_validated_units)
            logger.info(f'add_unit_to_poset {self.process_id} -> Validated a set of {len(list_of_validated_units)} units.')
            for tx in U.txs:
                if tx.issuer not in self.pending_txs.keys():
                    self.pending_txs[tx.issuer] = set()
                self.pending_txs[tx.issuer].add((tx,U))
            for V in list_of_validated_units:
                if V.txs:
                    newly_validated = self.validate_transactions_in_unit(V, U)
                    logger.info(f'add_unit_to_poset {self.process_id} -> Validated a set of {len(newly_validated)}/{len(V.txs)} transactions.')
                    self.validated_transactions.extend(newly_validated)
        else:
             return False

        return True

    def validate_transactions_in_unit(self, U, U_validator):
        '''
        Returns a list of transactions in U that can be fast-validated if U's validator unit is U_validator
        :param unit U: unit whose transactions should be validated
        :param unit U_validator: a unit that validates U (is high above U)
        :returns: list of all transactions in unit U that can be fast-validated
        '''
        logger = logging.getLogger(LOGGER_NAME)
        validated_transactions = []
        for tx in U.txs:
            user_public_key = tx.issuer

            assert user_public_key in self.pending_txs.keys(), f"No transaction is pending for user {user_public_key}."
            assert (tx, U) in self.pending_txs[user_public_key], "Transaction not found among pending"
            if tx.index != self.userDB.last_transaction(tx.issuer) + 1:
                logger.info(f'tx validation: transaction failed to validate because its index is {tx.index}, while the previous one was {self.userDB.last_transaction(tx.issuer)}')
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
            #else:
            #    print("fork")

        for tx in validated_transactions:
            self.pending_txs[tx.issuer].discard((tx,U))
        return validated_transactions


    async def create_add(self, txs_queue):
    #while True:
        for _ in range(10):
            txs = self.prepared_txs
            new_unit = self.poset.create_unit(self.process_id, txs, strategy = "link_self_predecessor", num_parents = 2)
            if new_unit is not None:
                assert self.poset.check_compliance(new_unit), "A unit created by our process is not passing the compliance test!"
                self.sign_unit(new_unit)
                #self.poset.add_unit(new_unit)
                self.add_unit_to_poset(new_unit)
                if not txs_queue.empty():
                    self.prepared_txs = txs_queue.get()
                else:
                    self.prepared_txs = []

            await asyncio.sleep(CREATE_FREQ)


    async def keep_syncing(self, executor):
        await asyncio.sleep(0.7)
        #while True:
        for _ in range(10):
            sync_candidates = list(range(self.n_processes))
            sync_candidates.remove(self.process_id)
            target_id = random.choice(sync_candidates)
            self.syncing_tasks.append(asyncio.create_task(sync(self, self.process_id, target_id, self.address_list[target_id], self.public_key_list, executor)))

            await asyncio.sleep(SYNC_INIT_FREQ)


    async def run(self):
        # start another process listening for incoming txs
        logger = logging.getLogger(LOGGER_NAME)

        txs_queue = multiprocessing.Queue()
        p = multiprocessing.Process(target=tx_listener, args=(self.tx_receiver_address, txs_queue))
        p.start()

        executor = concurrent.futures.ProcessPoolExecutor(max_workers=3)
        creator_task = asyncio.create_task(self.create_add(txs_queue))
        listener_task = asyncio.create_task(listener(self, self.process_id, self.address_list, self.public_key_list, executor))
        syncing_task = asyncio.create_task(self.keep_syncing(executor))

        await asyncio.gather(syncing_task, creator_task)
        logger.info(f'{self.process_id} gathered results; cancelling listener')
        listener_task.cancel()

        p.kill()
