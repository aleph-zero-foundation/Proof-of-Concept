import asyncio
import multiprocessing
import random

from optparse import OptionParser

from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.data_structures.userDB import UserDB
from aleph.network import tx_generator
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


async def main():

    parser = OptionParser()
    parser.add_option('-s', '--signing-keys', dest='signing_keys',
                      help="Location of processes' signing keys", metavar='KEYS')
    parser.add_option('-i', '--hosts', dest='hosts',
                      help='Ip addresses of processes', default='hosts')

    options, args = parser.parse_args()

    hosts_ip = read_hosts_ip(options.hosts)
    signing_keys = read_signing_keys(options.signing_keys)
    assert len(hosts_ip) == len(signing_keys), 'number of hosts and signing keys dont match!!!'
    n_processes = len(hosts_ip)
    public_keys = [VerifyKey.from_SigningKey(sk) for sk in signing_keys]
    process_id, public_keys, signing_keys, hosts_ip = sort_and_get_my_pid(public_keys, signing_keys, hosts_ip)
    addresses = [(ip, 8888) for ip in hosts_ip]
    sk, pk = signing_keys[process_id], public_keys[process_id]
    recv_address = ('127.0.0.1', 8888)
    userDB = prepare_DB()
    use_tcoin = True

    process = Process(n_processes, process_id, sk, pk, addresses, public_keys, recv_address,
                      userDB, 'LINEAR_ORDERING', use_tcoin)

    txps = 10


    p = multiprocessing.Process(target=tx_generator, args=([recv_address], userDB.user_balance.keys(), txps))
    p.start()
    tasks = [asyncio.create_task(process.run())]
    await asyncio.gather(*tasks)
    p.kill()


if __name__ == '__main__':
    asyncio.run(main())
