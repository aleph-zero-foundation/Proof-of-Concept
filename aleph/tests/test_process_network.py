#!/usr/bin/env python3
from aleph.network import listener, sync
from aleph.data_structures import Poset
from aleph.process import Process
from aleph.crypto.keys import SigningKey, VerifyKey

import asyncio

import subprocess
import marshal

DISCOVERY_PORT = 49643

def get_priv_keys(keyfile):
    result = []
    with open(keyfile, "r") as the_file:
        for hexed in the_file.readlines():
            result.append(SigningKey(hexed[:-1].encode('utf8')))
    return result

def get_pub_keys(keyfile):
    result = []
    with open(keyfile, "r") as the_file:
        for hexed in the_file.readlines():
            result.append(hexed[:-1].encode('utf8'))
    return result

def make_discovery_response(priv_keys):
    result = {}
    process_port = DISCOVERY_PORT
    for priv_key in priv_keys:
        process_port += 1
        result[process_port] = VerifyKey.from_SigningKey(priv_key).to_hex()
    return result

def make_discovery_server(priv_keys):
    response = marshal.dumps(make_discovery_response(priv_keys))
    async def discovery_server(reader, writer):
        writer.write(str(len(response)).encode())
        writer.write(b'\n')
        writer.write(response)
        await writer.drain()
        writer.close()
        await writer.wait_closed()
    return discovery_server

def get_ip_candidates(ip_range):
    ips = subprocess.Popen(
            "nmap -oG - -p"+str(DISCOVERY_PORT)+" "+ip_range+" | awk '/49643\/open/{print $2}'",
            shell=True,
            stdout=subprocess.PIPE).stdout.read().split(b'\n')[:-1]
    return set(ips)

async def discover_at(ip, all_pub_keys):
    print("Discovering at "+ip.decode('utf8'))
    reader, writer = await asyncio.open_connection(ip, DISCOVERY_PORT)
    data = await reader.readuntil()
    n_bytes = int(data[:-1])
    data = await reader.read(n_bytes)
    keys_by_port = marshal.loads(data)
    result = {}
    for port, key in keys_by_port.items():
        if key in all_pub_keys:
            result[key] = (ip, port)
    return result

async def discover(all_pub_keys, ip_range):
    client_map = {}
    checked_ips = set()
    while len(client_map) < len(all_pub_keys):
        await asyncio.sleep(1)
        ip_candidates = get_ip_candidates(ip_range) - checked_ips
        discovery_tasks = [asyncio.create_task(discover_at(ip, all_pub_keys)) for ip in ip_candidates]
        new_clients = await asyncio.gather(*discovery_tasks)
        for client in new_clients:
            client_map.update(client)
        checked_ips.update(ip_candidates)
    return client_map

def process_client_map(client_map):
    addresses, public_keys = [], []
    for pub_key in sorted(client_map):
        public_keys.append(pub_key)
        addresses.append(client_map[pub_key])
    return addresses, public_keys

async def run_processes(client_map, priv_keys):
    addresses, public_key_hexes = process_client_map(client_map)
    public_keys = [VerifyKey.from_hex(pub_key) for pub_key in public_key_hexes]
    n_processes = len(public_keys)
    tasks = []
    for priv_key in priv_keys:
        pub_key = VerifyKey.from_SigningKey(priv_key)
        process_id = public_key_hexes.index(pub_key.to_hex())
        tx_receiver_address = (addresses[process_id][0], addresses[process_id][1]+32)
        new_process = Process(n_processes, process_id, priv_key, pub_key, addresses, public_keys, tx_receiver_address)
        new_process.poset = Poset(n_processes)
        tasks.append(asyncio.create_task(new_process.run()))
    await asyncio.sleep(1)
    await asyncio.gather(*tasks)

async def run():
    # arguments: private keys, public keys, our IP, IP range
    import sys
    assert(len(sys.argv) == 5)
    priv_keys_file = sys.argv[1]
    pub_keys_file = sys.argv[2]
    our_ip = sys.argv[3]
    ip_range = sys.argv[4]

    priv_keys = get_priv_keys(priv_keys_file)
    all_pub_keys = get_pub_keys(pub_keys_file)

    discovery_server = make_discovery_server(priv_keys)
    async def serve():
        server = await asyncio.start_server(discovery_server, our_ip, DISCOVERY_PORT)
        async with server:
            await server.serve_forever()
    server_task = asyncio.create_task(serve())

    client_map = await discover(all_pub_keys, ip_range)

    await run_processes(client_map, priv_keys)

if __name__ == '__main__':
    asyncio.run(run())
