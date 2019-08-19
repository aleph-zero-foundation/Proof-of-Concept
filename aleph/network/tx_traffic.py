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

import logging
import pickle
import pkg_resources
import random
import socket
import socketserver

from time import sleep, perf_counter as get_time

from aleph.data_structures import Tx
import aleph.const as consts


def tx_listener(listen_addr, queue):
    '''
    Start a TCP server on *listen_addr* and listen for incoming transactions.
    Put batches (lists) of transactions on *queue* every consts.CREATE_DELAY seconds or when the tx limit per unit (consts.TXPU)
    is reached, whichever comes first.
    '''
    tx_buffer = []
    prev_put_time = get_time()

    class TCPHandler(socketserver.BaseRequestHandler):
        def handle(self):
            nonlocal tx_buffer, prev_put_time
            logger.info(f'tx_server_establish | Connection with {self.client_address}')

            data = self.request.recv(1024)
            tx = pickle.loads(data)
            tx_buffer.append(tx)

            logger.info(f'tx_server_receive | Received from {self.client_address}')

            if len(tx_buffer) == consts.TXPU or get_time() - prev_put_time > consts.CREATE_DELAY:
                prev_put_time = get_time()
                logger.info(f'tx_server_enqueue | Putting {len(tx_buffer)} txs on queue')
                queue.put(tx_buffer)
                tx_buffer = []

    logger = logging.getLogger(consts.LOGGER_NAME)
    logger.info(f'tx_server_start | Starting on {listen_addr}')

    with socketserver.TCPServer(listen_addr, TCPHandler) as server:
        server.serve_forever()


def tx_source_gen(batch_size, txpu, seed=27091986, filename=None):
    '''
    Produces a simple tx generator.
    :param int batch_size: number of txs for a process to input into the system.
    :param int txpu: number of txs to be included in one unit.
    :param int seed: seed for random generator.
    :param str filename: path to file with names of txs senders and recipients (each in a separate line).
      If None, aleph/test/data/light_nodes_public_keys is used.
    '''

    if filename is None:
        filename = pkg_resources.resource_stream('aleph.test.data', 'light_nodes_public_keys')
        lines = [line.decode() for line in filename.readlines()]
    else:
        with open(filename) as f:
            lines = f.readlines()
    ln_public_keys = [line.rstrip('\n') for line in lines]

    def _tx_source(dummy, queue):
        '''
        Generates transactions in bundles of size txpu till batch_size is reached
        :param dummy: dummy argument needed for comatibility of args list with tx_listener()
        :param queue queue: queue for newly generated txs
        '''
        # ensure that batches are different
        random.seed(seed)

        produced = 0
        while produced < batch_size:
            offset = min(txpu, batch_size - produced)

            txs = []
            for _ in range(offset):
                source, target = random.sample(ln_public_keys, k=2)
                amount = random.randint(1, 30000)
                txs.append(Tx(source, target, amount))

            produced += offset
            queue.put(txs, block=True)

    return _tx_source


def tx_generator(committee_addresses, signing_keys, txps):
    '''
    Generate random transactions indefinitely.
    Issuer and receiver of each transactions are random integers 0 <= i <= len(signing_keys)

    NOTE: this is an old version of TX generator that is still used by some tests.
    '''
    n_light_nodes = len(signing_keys)
    counter = 0
    starts = get_time()

    while True:
        if counter == txps:
            counter = 0
            delta = get_time() - starts
            if delta < 1:
                sleep(1 - delta)
            starts = get_time()

        issuer_id = random.randrange(0, n_light_nodes)
        receiver_id = random.choice([uid for uid in range(n_light_nodes) if uid != issuer_id])
        amount = random.randrange(1, 100)
        tx = Tx(issuer_id, receiver_id, amount)
        data = pickle.dumps(tx)

        sent = False
        while not sent:
            com_addr = random.choice(committee_addresses)

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                try:
                    sock.connect(com_addr)
                    sock.sendall(data)
                    sent = True
                except:
                    # assume any failure means that others have stopped
                    return

        counter += 1
