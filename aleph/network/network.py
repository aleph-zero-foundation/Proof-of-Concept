import asyncio
import pickle
import zlib
import socket

from .channel import Channel
from aleph.utils import timer
import aleph.const as consts


class Network:

    def __init__(self, process, addresses, public_key_list, logger):
        self.process = process
        self.addresses = addresses
        self.public_key_list = public_key_list
        self.logger = logger

        self.n_recv_syncs = 0
        pid = self.process.process_id
        self.sync_channels = {i: Channel(pid, i, addr) for i, addr in enumerate(addresses) if i != pid}
        self.listen_channels = {i: Channel(pid, i, addr) for i, addr in enumerate(addresses) if i != pid}


    async def start_server(self, server_started):

        async def channel_handler(reader, writer):
            self.logger.info(f'channel_handler {self.process.process_id} | Receiving connection from unknown process')

            peer_id = await Channel.receive_handshake(reader, writer)
            self.logger.info(f'channel_handler {self.process.process_id} | Exchanged handshake with {peer_id}')
            channel = self.listen_channels[peer_id]

            if channel.is_active():
                self.logger.error(f'channel_handler {self.process.process_id} | Channel with {peer_id} already open, received another connection')
                return

            channel.connect(reader, writer)
            self.logger.info(f'channel_handler {self.process.process_id} | Opened channel with {peer_id}')

        host_ip = socket.gethostbyname(socket.gethostname())
        host_port = self.addresses[self.process.process_id][1]
        server = await asyncio.start_server(channel_handler, host_ip, host_port)
        server_started.set()

        self.logger.info(f'server_start {self.process.process_id} | Starting sync server on {host_ip}:{host_port}')

        async with server:
            await server.serve_forever()


    async def sync(self, target_id):
        ids = self._get_ids_as_str()
        self.logger.info(f'sync_establish {ids} | Beginning sync with {target_id}')

        channel = self.sync_channels[target_id]

        self.process.last_synced_with_process[target_id] = self.process.sync_id
        self.process.sync_id += 1

        await self._send_poset_info(channel, 'sync', ids)
        their_poset_info = await self._receive_poset_info(channel, 'sync', ids)

        await self._send_units(their_poset_info, channel, 'sync', ids)
        units_received = await self._receive_units(channel, 'sync', ids)

        if not self._verify_signatures_and_add_units(units_received, target_id, 'sync', ids):
            return

        self.logger.info(f'sync_done {ids} | Syncing with {target_id} successful')
        timer.write_summary(where=self.logger, groups=[ids])


    async def listener(self, peer_id):
        channel = self.listen_channels[peer_id]
        while True:
            their_poset_info = await self._receive_poset_info(channel, 'listener', f'{self.process.process_id} ???')
            self.n_recv_syncs += 1
            ids = self._get_ids_as_str()

            self.process.last_synced_with_process[peer_id] = self.process.sync_id
            self.process.sync_id += 1

            self.logger.info(f'listener_sync_no {ids} | Number of syncs is {self.n_recv_syncs}')
            if self.n_recv_syncs > consts.N_RECV_SYNC:
                self.logger.info(f'listener_too_many_syncs {ids} | Too many syncs, rejecting {peer_id}')
                self.n_recv_syncs -= 1
                return
            self.logger.info(f'listener_establish {ids} | Connection established with {peer_id}')

            await self._send_poset_info(channel, 'listener', ids)

            units_received = await self._receive_units(channel, 'listener', ids)
            if not self._verify_signatures_and_add_units(units_received, peer_id, 'listener', ids):
                self.n_recv_syncs -= 1
                return

            await self._send_units(their_poset_info, channel, 'listener', ids)

            self.logger.info(f'listener_succ {ids} | Syncing with {peer_id} succesful')
            timer.write_summary(where=self.logger, groups=[ids])
            self.n_recv_syncs -= 1


    async def _send_poset_info(self, channel, mode, ids):
        self.logger.info(f'send_poset_{mode} {ids} | sending info about heights to {channel.peer_id}')
        poset_info = self.process.poset.get_heights()
        data = pickle.dumps((self.process.process_id, poset_info))
        if consts.SEND_COMPRESSED:
            data = zlib.compress(data)
        await channel.write(data)
        self.logger.info(f'send_poset_{mode} {ids} | sent heights {poset_info} to {channel.peer_id}')


    async def _receive_poset_info(self, channel, mode, ids):
        self.logger.info(f'receive_poset_{mode} {ids} | Receiving info about forkers and heights&hashes from {channel.peer_id}')
        data = await channel.read()
        if consts.SEND_COMPRESSED:
            data = zlib.decompress(data)
        peer_id, poset_info = pickle.loads(data)
        assert peer_id != self.process.process_id, "It seems we are syncing with ourselves."
        assert peer_id == channel.peer_id, "Incorrect process id sent with poset info."
        self.logger.info(f'receive_poset_{mode} {ids} | Got forkers/heights {poset_info} from {peer_id}')
        return poset_info


    async def _send_units(self, ex_heights, channel, mode, ids):
        self.logger.info(f'send_units_start_{mode} {ids} | Sending units to {channel.peer_id}')

        int_heights = self.process.poset.get_heights()

        units_to_send = []
        for i, (int_height, ex_height) in enumerate(zip(int_heights, ex_heights)):
            if int_height > ex_height:
                units = self.process.poset.units_by_height_interval(creator_id=i, min_height=ex_height+1, max_height=int_height)
                units_to_send.extend(units)
        units_to_send = self.process.poset.order_units_topologically(units_to_send)

        with timer(ids, 'pickle_units'):
            data = pickle.dumps(units_to_send)

        if consts.SEND_COMPRESSED:
            initial_len = len(data)
            with timer(ids, 'compress_units'):
                data = zlib.compress(data)
            compressed_len = len(data)
            gained = 1.0 - compressed_len/initial_len
            self.logger.info(f'compression_rate {ids} | Compressed {initial_len} to {compressed_len}, gained {gained:.4f} of size.')

        self.logger.info(f'send_units_wait_{mode} {ids} | Sending {len(units_to_send)} units and {len(data)} bytes to {channel.peer_id}')
        await channel.write(data)
        self.logger.info(f'send_units_sent_{mode} {ids} | Sent {len(units_to_send)} units and {len(data)} bytes to {channel.peer_id}')
        #previously there was a drain() call here, now it's a part of channel.write() Maybe this log below could be removed?
        self.logger.info(f'send_units_done_{mode} {ids} | Units sent {channel.peer_id}')


    async def _receive_units(self, channel, mode, ids):
        self.logger.info(f'receive_units_start_{mode} {ids} | Receiving units from {channel.peer_id}')
        data = await channel.read()
        n_bytes = len(data)
        self.logger.info(f'receive_units_bytes_{mode} {ids} | Received {n_bytes} bytes from {channel.peer_id}')

        if consts.SEND_COMPRESSED:
            with timer(ids, 'decompress_units'):
                data = zlib.decompress(data)

        with timer(ids, 'unpickle_units'):
            units_received = pickle.loads(data)

        self.logger.info(f'receive_units_done_{mode} {ids} | Received {n_bytes} bytes and {len(units_received)} units')
        return units_received


    def _verify_signatures(self, units_received, mode, ids):
        self.logger.info(f'verify_sign_{mode} {ids} | Verifying signatures')

        for unit in units_received:
            if not self.public_key_list[unit.creator_id].verify_signature(unit.signature, unit.bytestring()):
                return False

        self.logger.info(f'verify_sign_{mode} {ids} | Signatures verified')
        return True


    def _add_units(self, units_received, peer_id, mode, ids):
        self.logger.info(f'add_received_{mode} {ids} | trying to add {len(units_received)} units from {peer_id} to poset')
        printable_unit_hashes = ''

        for unit in units_received:
            self.process.poset.dehash_parents(unit)
            printable_unit_hashes += (' ' + unit.short_name())
            if not self.process.add_unit_to_poset(unit):
                self.logger.error(f'add_received_fail_{mode} {ids} | unit {unit.short_name()} from {peer_id} was rejected')
                return False

        self.logger.info(f'add_received_done_{mode} {ids} | units from {peer_id} were added succesfully {printable_unit_hashes} ')
        return True


    def _verify_signatures_and_add_units(self, units_received, peer_id, mode, ids):
        with timer(ids, 'verify_signatures'):
            succesful = self._verify_signatures(units_received, mode, ids)
        if not succesful:
            self.logger.error(f'{mode}_invalid_sign {ids} | Got a unit from {target_id} with invalid signature; aborting')
            return False

        with timer(ids, 'add_units'):
            succesful = self._add_units(units_received, peer_id, mode, ids)
        if not succesful:
            self.logger.error(f'{mode}_not_compliant {ids} | Got unit from {target_id} that does not comply to the rules; aborting')
            return False
        return True


    def _get_ids_as_str(self):
        return f'{self.process.process_id} {self.process.sync_id}'

