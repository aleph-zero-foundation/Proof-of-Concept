import asyncio
import pickle
import socket

from .channel import Channel, RejectException
from aleph.utils import timer
from aleph.actions import poset_info, units_to_send, dehash_parents
from aleph.data_structures import pretty_hash
import aleph.const as consts


class Network:

    def __init__(self, process, addresses, public_key_list, logger, keep_connection=True):
        '''Class that takes care of handling network connections with other processes.

        :param Process process: process who uses this network to communicate with others
        :param list addresses: list of addresses of all committee members, ordered by their process_id. Each address is a pair (IP, port)
        :param list public_key_list: the list of public keys of all committee members
        :param Logger logger: where to write all the log messages
        :param bool keep_connection: Don't close network connection after every sync
        '''

        self.process = process
        self.addresses = addresses
        self.public_key_list = public_key_list
        self.logger = logger
        self.keep_connection = keep_connection

        self.n_recv_syncs = 0
        self.n_init_syncs = 0
        pid = self.process.process_id
        self.sync_channels = {i: Channel(pid, i, addr) for i, addr in enumerate(addresses) if i != pid}
        self.listen_channels = {i: Channel(pid, i, addr) for i, addr in enumerate(addresses) if i != pid}

    async def start_server(self, server_started):
        '''
        Start a server that waits for incoming connections and activates corresponding listen_channels
        :param asyncio.Event server_started: main Event used to synchronize initialization of the whole process.run. This method sets it if successful.
        '''

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


    def _new_sync_id(self, peer_id):
        '''
        Increase sync_id counter in the parent process and register the current sync as a sync with process peer_id.
        Return a string identifier of that sync. The identifier is of the form 'process_id sync_id'.
        '''
        s = f'{self.process.process_id} {self.process.sync_id}'
        self.process.last_synced_with_process[peer_id] = self.process.sync_id
        self.process.sync_id += 1
        return s


    async def maybe_close(self, channel):
        if not self.keep_connection:
            await channel.close()


    async def _send_poset_info(self, channel, mode, ids):
        self.logger.info(f'send_poset_{mode} {ids} | sending info about heights to {channel.peer_id}')
        to_send = poset_info(self.process.poset)
        data = pickle.dumps(to_send)
        self.logger.info(f'send_poset_wait_{mode} {ids} | writing info about heights to {channel.peer_id}')
        await channel.write(data)
        printable_heights = [[(h, pretty_hash(H)) for (h, H) in local_info] for local_info in to_send]
        self.logger.info(f'send_poset_done_{mode} {ids} | sent heights {printable_heights} ({len(data)} bytes) '
                         f'to {channel.peer_id}')


    async def _receive_poset_info(self, channel, mode, ids):
        data = await channel.read()
        if ids is None:
            ids = self._new_sync_id(channel.peer_id)
        self.logger.info(f'receive_poset_{mode} {ids} | Receiving info about heights from {channel.peer_id}')
        info = pickle.loads(data)
        printable_heights = [[(h, pretty_hash(H)) for (h, H) in local_info] for local_info in info]
        self.logger.info(f'receive_poset_{mode} {ids} | Got heights {printable_heights} ({len(data)} bytes) '
                         f'from {channel.peer_id}')
        return info, ids


    async def _send_requests(self, to_send, channel, mode, ids):
        self.logger.info(f'send_requests_start_{mode} {ids} | Sending requests to {channel.peer_id}')
        data = pickle.dumps(to_send)
        self.logger.info(f'send_requests_wait_{mode} {ids} | writing requests to {channel.peer_id}')
        await channel.write(data)
        printable_requests = [[pretty_hash(H) for H in local_info] for local_info in to_send]
        self.logger.info(f'send_requests_done_{mode} {ids} | sent requests {printable_requests} ({len(data)} bytes) '
                         f'to {channel.peer_id}')


    async def _receive_requests(self, channel, mode, ids):
        self.logger.info(f'receive_requests_start_{mode} {ids} | receiving requests from {channel.peer_id}')
        data = await channel.read()
        requests_received = pickle.loads(data)
        printable_requests = [[pretty_hash(H) for H in local_info] for local_info in requests_received]
        self.logger.info(f'receive_requests_done_{mode} {ids} | received requests {printable_requests} ({len(data)} bytes) '
                         f'from {channel.peer_id}')
        return requests_received


    async def _send_units(self, to_send, channel, mode, ids):
        self.logger.info(f'send_units_start_{mode} {ids} | Sending units to {channel.peer_id}')
        with timer(ids, 'pickle_units'):
            data = pickle.dumps(to_send)
        self.logger.info(f'send_units_wait_{mode} {ids} | Sending {len(to_send)} units and {len(data)} bytes to {channel.peer_id}')
        await channel.write(data)
        self.logger.info(f'send_units_sent_{mode} {ids} | Sent {len(to_send)} units and {len(data)} bytes to {channel.peer_id}')
        self.logger.info(f'send_units_done_{mode} {ids} | Units sent {channel.peer_id}')


    async def _receive_units(self, channel, mode, ids):
        self.logger.info(f'receive_units_start_{mode} {ids} | Receiving units from {channel.peer_id}')
        data = await channel.read()
        n_bytes = len(data)
        self.logger.info(f'receive_units_bytes_{mode} {ids} | Received {n_bytes} bytes from {channel.peer_id}')
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
            dehash_parents(self.process.poset, unit)
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
            self.logger.error(f'{mode}_invalid_sign {ids} | Got a unit from {peer_id} with invalid signature; aborting')
            return False

        with timer(ids, 'add_units'):
            succesful = self._add_units(units_received, peer_id, mode, ids)
        if not succesful:
            self.logger.error(f'{mode}_not_compliant {ids} | Got unit from {peer_id} that does not comply to the rules; aborting')
            return False
        return True


#===============================================================================================================================
# SYNCING PROTOCOLS
#===============================================================================================================================

    async def sync(self, peer_id):
        '''
        Sync with process peer_id.
        This version uses 3-exchange "pullpush" protocol: send heights, receive heights, units and requests, send units and requests.
        If we sent some requests there is a 4th exchange where we once again get units. This should only happen due to forks.
        '''
        self.n_init_syncs += 1
        self.logger.info(f'sync_sync_no | Number of syncs is {self.n_init_syncs}')
        if self.n_init_syncs > consts.N_INIT_SYNC:
            self.logger.info(f'sync_too_many_syncs | Too many syncs, not initiating a new one with {peer_id}')
            self.n_init_syncs -= 1
            return
        channel = self.sync_channels[peer_id]
        if channel.in_use.locked():
            self.logger.info(f'sync_canceled {self.process.process_id} | Previous sync with {peer_id} still in progress')
            self.n_init_syncs -= 1
            return

        async with channel.in_use:
            ids = self._new_sync_id(peer_id)
            self.logger.info(f'sync_establish_try {ids} | Establishing connection to {peer_id}')
            self.logger.info(f'sync_establish {ids} | Established connection to {peer_id}')

            #step 1
            await self._send_poset_info(channel, 'sync', ids)

            #step 2
            try:
                their_poset_info, _ = await self._receive_poset_info(channel, 'sync', ids)
                units_received = await self._receive_units(channel, 'sync', ids)
                their_requests = await self._receive_requests(channel, 'sync', ids)
            except RejectException:
                self.logger.info(f'sync_rejected {ids} | Process {peer_id} rejected sync attempt')
                self.n_init_syncs -= 1
                await self.maybe_close(channel)
                return

            #step 3
            with timer(ids, 'prepare_units'):
                to_send, to_request = units_to_send(self.process.poset, their_poset_info, their_requests)
            await self._send_units(to_send, channel, 'sync', ids)
            received_hashes = [U.hash() for U in units_received]
            to_request = [[r for r in reqs if r not in received_hashes] for reqs in to_request]
            await self._send_requests(to_request, channel, 'sync', ids)

            #step 4 (only if we requested something)
            if any(to_request):
                self.logger.info(f'sync_extended {ids} | Sync with {peer_id} extended due to forks')
                units_received = await self._receive_units(channel, 'sync', ids)

        await self.maybe_close(channel)

        if self._verify_signatures_and_add_units(units_received, peer_id, 'sync', ids):
            self.logger.info(f'sync_succ {ids} | Syncing with {peer_id} successful')
            timer.write_summary(where=self.logger, groups=[ids])
        else:
            self.logger.info(f'sync_fail {ids} | Syncing with {peer_id} failed')
        self.n_init_syncs -= 1


    async def listener(self, peer_id):
        '''
        Listen indefinitely for incoming syncs from process peer_id.
        This version is a counterpart for sync, follows the same 3-exchange protocol.
        '''
        channel = self.listen_channels[peer_id]
        while True:
            #step 1
            their_poset_info, ids = await self._receive_poset_info(channel, 'listener', None)

            self.n_recv_syncs += 1
            self.logger.info(f'listener_sync_no {ids} | Number of syncs is {self.n_recv_syncs}')

            if self.n_recv_syncs > consts.N_RECV_SYNC:
                self.logger.info(f'listener_too_many_syncs {ids} | Too many syncs, rejecting {peer_id}')
                await channel.reject()
                await self.maybe_close(channel)
                self.n_recv_syncs -= 1
                continue

            self.logger.info(f'listener_establish {ids} | Connection established with {peer_id}')

            #step 2
            await self._send_poset_info(channel, 'listener', ids)
            with timer(ids, 'prepare_units'):
                to_send, to_request = units_to_send(self.process.poset, their_poset_info)
            await self._send_units(to_send, channel, 'listener', ids)
            await self._send_requests(to_request, channel, 'listener', ids)

            #step 3
            units_received = await self._receive_units(channel, 'listener', ids)
            their_requests = await self._receive_requests(channel, 'listener', ids)

            #step 4 (only if they requested something)
            if any(their_requests):
                self.logger.info(f'listener_extended {ids} | Sync with {peer_id} extended due to forks')
                with timer(ids, 'prepare_units'):
                    to_send, _ = units_to_send(self.process.poset, their_poset_info, their_requests)
                await self._send_units(to_send, channel, 'listener', ids)

            if self._verify_signatures_and_add_units(units_received, peer_id, 'listener', ids):
                self.logger.info(f'listener_succ {ids} | Syncing with {peer_id} successful')
                timer.write_summary(where=self.logger, groups=[ids])
            else:
                self.logger.info(f'listener_fail {ids} | Syncing with {peer_id} failed')

            await self.maybe_close(channel)
            self.n_recv_syncs -= 1
