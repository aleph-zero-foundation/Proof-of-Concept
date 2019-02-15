import asyncio
import multiprocessing
import random
import socket

from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.data_structures import Unit, UserDB
from aleph.network import tx_generator
from aleph.process import Process
from byzantine_process import ByzantineProcess
import aleph.const as consts


async def execute_test(node_builder=Process, start_port=8900, tx_receiver_address_start_port=9100):
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
                                   gossip_strategy = 'non_recent_random')
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

    def __init__(self,
                 n_processes,
                 process_id,
                 secret_key, public_key,
                 address_list,
                 public_key_list,
                 tx_receiver_address,
                 userDB=None,
                 validation_method='SNAP',
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
        self.process_copy = Process(n_processes,
                                    process_id,
                                    secret_key, public_key,
                                    address_list,
                                    public_key_list,
                                    tx_receiver_address,
                                    userDB,
                                    validation_method,
                                    gossip_strategy=gossip_strategy)
        self.has_stopped_adding = False

    def handle_byzantine_state(self, unit, forking_unit):
        ByzantineProcess.handle_byzantine_state(self, unit, forking_unit)

        self.stop_adding_and_syncing()
        sync_candidates = list(range(self.n_processes))
        sync_candidates.remove(self.process_id)
        sync_candidates = sync_candidates * 2 if len(sync_candidates) < 2 else sync_candidates
        if len(sync_candidates) == 0:
            self.logger.debug('no candidate to whom I can send units')
            return
        target_ids = random.sample(sync_candidates, 2)

        async def sync_wrapper():
            self.logger.debug('syncing the first local view of the poset')
            await self.network.sync(target_ids[0])
            self.logger.debug('first forking view of the poset was synced succesfully')

            # NOTE: this is some king of a dirty hack. We tried creating a new instance of Network using the second process, but
            # due to how channels are handled we were not able to use it - peers were rejecting new channels from already
            # connected nodes.

            # switch the instance of the process used by the network instance
            self.logger.debug('syncing the second local view of the poset')
            self.network.process = self.process_copy
            await self.network.sync(target_ids[1])
            self.logger.debug('second forking view of the poset was synced succesfully')

            self.network.process = self
            self.disable()

        asyncio.create_task(sync_wrapper())

    def stop_adding_and_syncing(self):
        self.has_stopped_adding = True
        self.keep_syncing = False

    def can_add(self):
        return not self.has_stopped_adding

    def add_byzantine_unit(self, process, unit):
        return ByzantineProcess.add_byzantine_unit(self, self.process_copy, unit)

    def translate_unit(self, U, process):
        parent_hashes = [V.hash() for V in U.parents]
        parents = [process.poset.units[V] for V in parent_hashes]
        U_new = Unit(U.creator_id, parents, U.transactions(), U.coin_shares)
        process.sign_unit(U_new)
        return U_new

    def add_unit_to_poset(self, U):
        if not self.can_add():
            return False
        if ByzantineProcess.add_unit_to_poset(self, U):
            U_new = self.translate_unit(U, self.process_copy)
            return self.process_copy.add_unit_to_poset(U_new)
        return False


class ForkDivideAndStayAliveProcess(ForkDivideAndDieProcess):

    def disable(self):
        pass


def is_byzantine(process_id):
    return process_id == 0


def is_process_byzantine(process):
    return is_byzantine(process.process_id)


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


if __name__ == '__main__':
    print('executing the ByzantineProcess test')
    asyncio.run(execute_test(process_builder(ByzantineProcess), 8900, 9100))
    print('success')

    print('executing the ForkDivideAndStayAliveProcess test')
    asyncio.run(execute_test(process_builder(ForkDivideAndStayAliveProcess), 9300, 9500))
    print('success')

    print('executing the ForkDivideAndDieProcess test')
    asyncio.run(execute_test(process_builder(ForkDivideAndDieProcess), 9700, 9900))
    print('success')
