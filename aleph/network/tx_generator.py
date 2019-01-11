import pickle
import random
import socket
import time

from aleph.crypto import SigningKey, VerifyKey
from aleph.data_structures import Tx


def tx_generator(committee_addresses, signing_keys, txps):
    n_light_nodes = len(signing_keys)
    last_tx_index = [-1 for _ in range(n_light_nodes)]
    #signing Txs temporarily disabled
    #verify_keys_hex = [VerifyKey.from_SigningKey(sk).to_hex() for sk in signing_keys]

    counter = 0
    starts = time.time()

    while True:
        if counter == txps:
            counter = 0
            if time.time() - starts < 1:
                time.sleep(1-(time.time()-starts))
            starts = time.time()

        issuer_id = random.randrange(0, n_light_nodes)
        receiver_id = random.choice([uid for uid in range(n_light_nodes) if uid != issuer_id])
        amount = random.randrange(1, 100)
        index = last_tx_index[issuer_id] + 1
        tx = Tx(issuer_id, receiver_id, amount, index)
        data = pickle.dumps(tx, protocol=4)

        sent = False
        while not sent:
            com_addr = random.choice(committee_addresses)

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                try:
                    sock.connect(com_addr)
                    sock.sendall(data)
                    sent = True
                except:
                    return # assume all failures mean others stopped

        last_tx_index[issuer_id] += 1
        counter += 1
