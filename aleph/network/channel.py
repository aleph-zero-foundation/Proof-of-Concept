import asyncio

class Channel:

    def __init__(self, owner_id, peer_id, peer_address):
        self.owner_id = owner_id
        self.peer_id = peer_id
        self.address = peer_address
        self.active = asyncio.Event()
        self.reader = None
        self.writer = None


    @staticmethod
    async def receive_handshake(reader, writer):
        data = await reader.readuntil()
        return int(data[:-1])


    def send_handshake(self):
        self.writer.write(f'{self.owner_id}\n'.encode())


    def connect(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.active.set()


    def is_active(self):
        return self.active.is_set()


    async def read(self):
        await self.active.wait()

        data = await self.reader.readuntil()
        n_bytes = int(data[:-1])
        data = await self.reader.readexactly(n_bytes)
        return data


    async def write(self, data):
        if not self.is_active():
            await self.open()

        self.writer.write(str(len(data)).encode())
        self.writer.write(b'\n')
        self.writer.write(data)
        await self.writer.drain()


    async def open(self):
        self.reader, self.writer = await asyncio.open_connection(*self.address)
        self.send_handshake()
        self.active.set()


    async def close(self):
        if self.is_active():
            self.writer.close()
            await self.writer.wait_closed()
            self.reader, self.writer = None, None
            self.active.clear()
