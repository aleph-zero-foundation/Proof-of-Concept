import asyncio
import configparser
import multiprocessing
import random
import sys

from optparse import OptionParser

from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.data_structures import UserDB, Tx
from aleph.network import tx_listener, tx_source_gen
from aleph.process import Process

import aleph.const as consts


def read_ip_addresses(ip_addresses_path):
    with open(ip_addresses_path, 'r') as f:
        return [line[:-1] for line in f]


def read_signing_keys(signing_keys_path):
    with open(signing_keys_path, 'r') as f:
        hexes = [line[:-1].encode() for line in f]
        return [SigningKey(hexed) for hexed in hexes]


def sort_and_get_my_pid(public_keys, signing_keys, my_ip, ip_addresses):
    ind = ip_addresses.index(my_ip)
    my_pk = public_keys[ind]

    pk_hexes = [pk.to_hex() for pk in public_keys]
    arg_sort = [i for i, _ in sorted(enumerate(pk_hexes), key = lambda x: x[1])]
    public_keys = [public_keys[i] for i in arg_sort]
    signing_keys = [signing_keys[i] for i in arg_sort]
    ip_addresses = [ip_addresses[i] for i in arg_sort]

    return public_keys.index(my_pk), public_keys, signing_keys, ip_addresses


def update_global_consts(params):
    ''' updates global consts defined in aleph/const.py by values in params '''
    for const_name in consts.__dict__:
        if const_name in params:
            consts.__dict__[const_name] = params[const_name]


async def main():
    if len(sys.argv) < 2:
        print('Specifiy path to .ini file')
        sys.exit(1)

    ini_path = sys.argv[1]
    params = configparser.ConfigParser()
    params.read(ini_path)

    update_global_consts(params)

    ip_addresses = read_ip_addresses(params['ip_addresses'])
    signing_keys = read_signing_keys(params['signing_keys'])

    assert len(ip_addresses) == len(signing_keys), 'number of hosts and signing keys dont match!!!'
    public_keys = [VerifyKey.from_SigningKey(sk) for sk in signing_keys]

    process_id, public_keys, signing_keys, ip_addresses = sort_and_get_my_pid(public_keys, signing_keys, params['my_ip'], ip_addresses)

    sk, pk = signing_keys[process_id], public_keys[process_id]

    n_processes = len(ip_addresses)
    userDB = None

    recv_address = None
    if params['tx_source'] == 'tx_source_gen':
        tx_source = tx_source_gen(params['tx_limit'], seed=process_id)
    else:
        tx_source = tx_listener

    process = Process(n_processes,
                      process_id,
                      sk, pk,
                      ip_addresses,
                      public_keys,
                      recv_address,
                      userDB,
                      'LINEAR_ORDERING',
                      tx_source)

    await process.run()


if __name__ == '__main__':
    asyncio.run(main())
