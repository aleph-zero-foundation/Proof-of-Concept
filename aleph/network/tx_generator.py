import marshal
import random
import socket
import time

from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.data_structures import tx_to_message


def tx_generator(committee_addresses, n_light_nodes, txps):
    last_tx_index = [-1 for _ in range(n_light_nodes)]
    signing_keys = [SigningKey() for _ in range(n_light_nodes)]
    verify_keys_hex = [VerifyKey.from_SigningKey(sk).to_hex() for sk in signing_keys]

    counter = 0
    starts = time.time()

    while True:
        if counter == txps:
            counter = 0
            if time.time() - starts < 1:
                time.sleep(1-(time.time()-starts))
            starts = time.time()

        com_addr = random.choice(committee_addresses)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(com_addr)

            issuer_id = random.randrange(0, n_light_nodes)
            receiver_id = random.choice([uid for uid in range(n_light_nodes) if uid != issuer_id])
            tx = {'issuer': verify_keys_hex[issuer_id],
                  'amount': 0,
                  'receiver': verify_keys_hex[receiver_id],
                  'index': last_tx_index[issuer_id] + 1}
            message = tx_to_message(tx['issuer'], tx['amount'], tx['receiver'], tx['index'])
            tx['signature'] = signing_keys[issuer_id].sign(message)
            last_tx_index[issuer_id] += 1
            data = marshal.dumps(tx)
            sock.sendall(data)
            counter += 1
