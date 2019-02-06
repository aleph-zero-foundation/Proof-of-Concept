import pickle
import random
import socket

from time import sleep, perf_counter as get_time

from aleph.crypto import SigningKey, VerifyKey
from aleph.data_structures import Tx


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
                    return # assume any failure means that others have stopped

        counter += 1

