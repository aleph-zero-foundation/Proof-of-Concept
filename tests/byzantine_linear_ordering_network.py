import asyncio
import multiprocessing
import random
import socket

from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.data_structures import Unit, UserDB
from aleph.network import tx_generator, Network
from aleph.process import Process
from byzantine_process import ByzantineProcess
import aleph.const as consts


async def execute_test(node_builder=Process, start_port=8900, tx_receiver_address_start_port=9100):
    n_processes = 4
    txps = 50
    n_light_nodes = 100
    consts.LEVEL_LIMIT = 5

    processes = []
    host_ports = [start_port+i for i in range(n_processes)]
    local_ip = socket.gethostbyname(socket.gethostname())
    addresses = [(local_ip, port) for port in host_ports]
    recv_addresses = [(local_ip, tx_receiver_address_start_port+i) for i in range(n_processes)]

    signing_keys = [SigningKey() for _ in range(n_processes)]
    public_keys = [VerifyKey.from_SigningKey(sk) for sk in signing_keys]

    tasks = []

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
        processes.append(new_process)
        tasks.append(asyncio.create_task(new_process.run()))

    await asyncio.sleep(1)

    p = multiprocessing.Process(target=tx_generator, args=(recv_addresses, ln_signing_keys, txps))
    p.start()

    await asyncio.gather(*tasks)
    p.kill()


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

        self.stop_adding()
        sync_candidates = list(range(self.n_processes))
        sync_candidates.remove(self.process_id)
        sync_candidates = sync_candidates * 2 if len(sync_candidates) < 2 else sync_candidates
        if len(sync_candidates) == 0:
            self.logger.debug('no candidate to whom I can send units')
            return
        target_ids = random.sample(sync_candidates, 2)

        async def sync_wrapper():
            async def sync_1_fun():
                self.logger.debug('syncing the first local view of the poset')
                await self.network.sync(target_ids[0])
                self.logger.debug('first forking view of the poset was synced succesfully')

            sync_1 = asyncio.create_task(sync_1_fun())

            async def sync_2_fun():
                self.logger.debug('syncing the second local view of the poset')
                tmp_network = Network(self.process_copy, self.addresses, self.public_key_list, self.logger)
                await tmp_network.sync(target_ids[1])
                self.logger.debug('second forking view of the poset was synced succesfully')

            sync_2 = asyncio.create_task(sync_2_fun())

            await asyncio.gather(sync_1, sync_2)

            self.disable()

        asyncio.create_task(sync_wrapper())

    def stop_adding(self):
        self.has_stopped_adding = True

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
        if process_id == 0:
            creator = byzantine_builder

        return creator(n_processes, process_id, sk, pk, addresses, public_keys, recv_address, userDB,
                       validation_method, gossip_strategy=gossip_strategy)

    return byzantine_process_builder


if __name__ == '__main__':
    print('executing the ByzantineProcess test')
    asyncio.run(execute_test(process_builder(ByzantineProcess), 8900, 9100))
    print('success')

    print('executing the ForkDivideAndDieProcess test')
    asyncio.run(execute_test(process_builder(ForkDivideAndDieProcess), 9300, 9500))
    print('success')
