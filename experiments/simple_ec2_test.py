'''
    This is a Proof-of-Concept implementation of Aleph Zero consensus protocol.
    Copyright (C) 2019 Aleph Zero Team
    
    This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    
    You should have received a copy of the GNU General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>.
'''

import asyncio
import multiprocessing
import random

from optparse import OptionParser

from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.data_structures import UserDB, Tx
from aleph.process import Process


def read_hosts_ip(hosts_path):
    with open(hosts_path, 'r') as f:
        return [line[:-1] for line in f]


def read_signing_keys(signing_keys_path):
    with open(signing_keys_path, 'r') as f:
        hexes = [line[:-1].encode() for line in f]
        return [SigningKey(hexed) for hexed in hexes]


def prepare_DB():
    random.seed(1729)
    initial_balances_and_indices = []
    with open('light_nodes_public_keys', 'r') as f:
        ln_public_keys = [line[:-1] for line in f]
    for i in range(len(ln_public_keys)):
        initial_balances_and_indices.append((ln_public_keys[i], random.randrange(10000, 100000), -1))

    return UserDB(initial_balances_and_indices)


def sort_and_get_my_pid(public_keys, signing_keys, hosts_ip):
    with open('my_ip') as f:
        my_ip = f.readline().strip()
    ind = hosts_ip.index(my_ip)
    my_pk = public_keys[ind]

    pk_hexes = [pk.to_hex() for pk in public_keys]
    arg_sort = [i for i, _ in sorted(enumerate(pk_hexes), key = lambda x: x[1])]
    public_keys = [public_keys[i] for i in arg_sort]
    signing_keys = [signing_keys[i] for i in arg_sort]
    hosts_ip = [hosts_ip[i] for i in arg_sort]

    return public_keys.index(my_pk), public_keys, signing_keys, hosts_ip


def tx_source_gen(process_id, n_processes, batch_size, txpu):
    '''
    Produces a tx generator and ensures that all processes choose batches from the same set.
    :param int batch_size: number of txs for a process to input into the system.
    :param int n_processes: number of parties.
    :param int txpu: number of txs to be included in one unit.
    '''

    def _tx_source(tx_receiver_address, tx_queue):
        '''
        Generates transactions in bundles of size txpu till batch_size is reached
        :param None tx_receiver_address: needed only for comatibility of args list with network.tx_listener
        :param queue tx_queue: queue for newly generated txs
        '''
        # ensure that batches are different
        random.seed(process_id)
        with open('light_nodes_public_keys', 'r') as f:
            ln_public_keys = [line[:-1] for line in f]

        proposed = 0
        while proposed<batch_size:
            if proposed+txpu <= batch_size:
                offset = txpu
            else:
                offset = batch_size - proposed

            txs = []
            for _ in range(offset):
                source = random.choice(ln_public_keys)
                target = random.choice(ln_public_keys)
                amount = random.randint(1, 32767)
                txs.append(Tx(source, target, amount))

            proposed += offset

            tx_queue.put(txs, block=True)

    return _tx_source


async def main():

    parser = OptionParser()
    parser.add_option('-k', '--signing-keys', dest='signing_keys', help="Location of processes' signing keys", metavar='KEYS')
    parser.add_option('-i', '--hosts', dest='hosts', help='Ip addresses of processes', default='hosts')
    parser.add_option('-c', '--n_create', dest='n_create', type=int, help='Number of units to create', default=-1, )
    parser.add_option('-s', '--n_sync', dest='n_sync', type=int, help='Number of syncs to be performed', default=-1)
    parser.add_option('-l', '--n_level', dest='n_level', type=int, help='Number of level units to create', default=-1)
    parser.add_option('-b', '--batch_size', dest='batch_size', type=int, help='Number of transactions to input to the system')
    parser.add_option('-u', '--txpu', dest='txpu', type=int, help='Number of transactions per unit')
    parser.add_option('-t', '--tcoin', dest='tcoin', type=int, help='Whether to use threshold coin')


    options, args = parser.parse_args()

    hosts_ip = read_hosts_ip(options.hosts)
    signing_keys = read_signing_keys(options.signing_keys)
    assert len(hosts_ip) == len(signing_keys), 'number of hosts and signing keys dont match!!!'
    public_keys = [VerifyKey.from_SigningKey(sk) for sk in signing_keys]

    process_id, public_keys, signing_keys, hosts_ip = sort_and_get_my_pid(public_keys, signing_keys, hosts_ip)
    addresses = [(ip, 8888) for ip in hosts_ip]
    sk, pk = signing_keys[process_id], public_keys[process_id]

    n_processes = len(hosts_ip)
    userDB = None
    use_tcoin = True
    stop_conditions = dict(zip(['n_create', 'n_sync', 'n_level'], [options.n_create, options.n_sync, options.n_level]))

    recv_address = None
    tx_source = tx_source_gen(process_id, n_processes, options.batch_size, options.txpu)

    process = Process(n_processes,
                      process_id,
                      sk, pk,
                      addresses,
                      public_keys,
                      recv_address,
                      userDB,
                      use_tcoin,
                      stop_conditions,
                      tx_source)

    await process.run()


if __name__ == '__main__':
    asyncio.run(main())
