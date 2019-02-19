import asyncio
import multiprocessing
import random
import socket

from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.data_structures import UserDB
from aleph.network import tx_generator
from aleph.process import Process
from byzantine_process import ByzantineProcess
import aleph.const as consts


def process_builder(byzantine_builder):
    def byzantine_process_builder(n_processes,
                                  process_id,
                                  sk, pk,
                                  addresses,
                                  public_keys,
                                  recv_address,
                                  userDB=None,
                                  validation_method='LINEAR_ORDERING',
                                  gossip_strategy='non_recent_random'):
        creator = Process
        if is_byzantine(process_id):
            creator = byzantine_builder

        return creator(n_processes, process_id, sk, pk, addresses, public_keys, recv_address, userDB,
                       validation_method, gossip_strategy=gossip_strategy)

    return byzantine_process_builder


async def execute_test(node_builder=process_builder(ByzantineProcess), start_port=8900, tx_receiver_address_start_port=9100):
    '''
    Executes a test consisting of spawning some number of instances of the Process class, including instances that are
    byzantine, generating some number of transactions and syncing all of the nodes until they reach some level of their posets.
    :param node_builder: a factory method producing instances of the class Process
    :param start_port: start of the port range used by created instances of the Process class for syncing
    :param tx_receiver_address_start_port: start of the port range used by created Process instances for receiving transactions
    '''
    n_processes = 4
    txps = 50
    n_light_nodes = 100
    consts.LEVEL_LIMIT = 10

    host_ports = [start_port+i for i in range(n_processes)]
    local_ip = socket.gethostbyname(socket.gethostname())
    addresses = [(local_ip, port) for port in host_ports]
    recv_addresses = [(local_ip, tx_receiver_address_start_port+i) for i in range(n_processes)]

    signing_keys = [SigningKey() for _ in range(n_processes)]
    public_keys = [VerifyKey.from_SigningKey(sk) for sk in signing_keys]

    tasks = []
    byzantine_tasks = []

    initial_balances_and_indices = []
    ln_signing_keys = [SigningKey() for _ in range(n_light_nodes)]
    ln_public_keys = [VerifyKey.from_SigningKey(sk) for sk in ln_signing_keys]
    for i in range(n_light_nodes):
        initial_balances_and_indices.append((ln_public_keys[i].to_hex(),
                                             random.randrange(10000, 100000),
                                             -1))
    userDB = UserDB(initial_balances_and_indices)

    for process_id in range(n_processes):
        sk = signing_keys[process_id]
        pk = public_keys[process_id]
        recv_address = recv_addresses[process_id]
        new_process = node_builder(n_processes, process_id, sk, pk, addresses,
                                   public_keys, recv_address, userDB,
                                   'LINEAR_ORDERING',
                                   gossip_strategy='unif_random')
        if is_process_byzantine(new_process):
            byzantine_tasks.append(asyncio.create_task(new_process.run()))
        else:
            tasks.append(asyncio.create_task(new_process.run()))

    await asyncio.sleep(1)

    p = multiprocessing.Process(target=tx_generator, args=(recv_addresses, ln_signing_keys, txps))
    p.start()
    await asyncio.gather(*tasks)
    p.kill()
    for byzantine_task in byzantine_tasks:
        byzantine_task.cancel()


class ForkDivideAndDieProcess(ByzantineProcess):
    '''
    Implementation of the byzantine process which after some given number of levels creates a fork, syncs two different versions
    of its poset with two random peers and immediately dies.
    '''

    def __init__(self,
                 n_processes,
                 process_id,
                 secret_key, public_key,
                 address_list,
                 public_key_list,
                 tx_receiver_address,
                 userDB=None,
                 validation_method='LINEAR_ORDERING',
                 gossip_strategy='unif_random'):
        ByzantineProcess.__init__(self,
                                  n_processes,
                                  process_id,
                                  secret_key, public_key,
                                  address_list,
                                  public_key_list,
                                  tx_receiver_address,
                                  userDB,
                                  validation_method,
                                  gossip_strategy=gossip_strategy)
        self._process_copy = Process(n_processes,
                                     process_id,
                                     secret_key, public_key,
                                     address_list,
                                     public_key_list,
                                     tx_receiver_address,
                                     userDB,
                                     validation_method,
                                     gossip_strategy=gossip_strategy)
        self._has_stopped_adding = False

    def _handle_byzantine_state(self, unit, forking_unit):
        ByzantineProcess._handle_byzantine_state(self, unit, forking_unit)

        self._stop_adding_and_syncing()
        sync_candidates = list(range(self.n_processes))
        sync_candidates.remove(self.process_id)
        sync_candidates = sync_candidates * 2 if len(sync_candidates) < 2 else sync_candidates
        if len(sync_candidates) == 0:
            self.logger.debug('no candidate to whom I can send units')
            return
        target_ids = random.sample(sync_candidates, 2)

        async def sync_wrapper():
            self._logger.debug('syncing the first local view of the poset')
            await self.network.sync(target_ids[0])
            self._logger.debug('first forking view of the poset was synced succesfully')

            # NOTE: this is some king of a dirty hack. We tried creating a new instance of Network using the second process, but
            # due to how channels are handled we were not able to use it - peers were rejecting new channels from already
            # connected nodes.

            # switch the instance of the Process class used by the network instance
            self._logger.debug('syncing the second local view of the poset')
            self.network.process = self._process_copy
            await self.network.sync(target_ids[1])
            self._logger.debug('second forking view of the poset was synced succesfully')

            self.network.process = self
            self.disable()

        asyncio.create_task(sync_wrapper())

    def _stop_adding_and_syncing(self):
        self._has_stopped_adding = True
        self._keep_syncing = False

    def _can_add(self):
        return not self._has_stopped_adding

    def _add_byzantine_unit(self, process, unit):
        return ByzantineProcess._add_byzantine_unit(self, self._process_copy, unit)

    def add_unit_to_poset(self, U):
        if not self._can_add():
            return False
        if ByzantineProcess.add_unit_to_poset(self, U):
            U_new = self.translate_unit(U, self._process_copy)
            return self._process_copy.add_unit_to_poset(U_new)
        return False


class ForkDivideAndStayAliveProcess(ForkDivideAndDieProcess):
    '''
    Type of a byzantine Process that keeps functioning after it creates a forking unit. It tries to use only one version of its
    forked poset.
    '''

    def disable(self):
        '''
        Overrides the method from the ByzantineProcess class to avoid disabling an instance after it created a fork.
        '''
        pass


def is_byzantine(process_id):
    return process_id == 0


def is_process_byzantine(process):
    return is_byzantine(process.process_id)


if __name__ == '__main__':
    print('executing the ByzantineProcess test')
    asyncio.run(execute_test(process_builder(ByzantineProcess), 7000, 7500))
    print('success')

    print('executing the ForkDivideAndStayAliveProcess test')
    asyncio.run(execute_test(process_builder(ForkDivideAndStayAliveProcess), 8000, 8500))
    print('success')

    print('executing the ForkDivideAndDieProcess test')
    asyncio.run(execute_test(process_builder(ForkDivideAndDieProcess), 9000, 9500))
    print('success')
