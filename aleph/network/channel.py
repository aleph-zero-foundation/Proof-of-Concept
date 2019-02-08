import asyncio

class Channel:

    def __init__(self, peer_id, address):
        self.peer_id = peer_id
        self.address = address
        self.reader = None
        self.writer = None


    def is_open(self):
        return self.reader is not None


    async def open(self, arg=None):
        self.reader, self.writer = arg if arg else await asyncio.open_connection(*self.address)


    async def close(self):
        if self.writer is not None:
            self.writer.close()
            await self.writer.wait_closed()
            self.reader, self.writer = None, None


    async def read(self):
        if not self.is_open():
            await self.open()

        data = await self.reader.readuntil()
        n_bytes = int(data[:-1])
        data = await self.reader.readexactly(n_bytes)
        return data


    async def write(self, data):
        if not self.is_open():
            await self.open()

        self.writer.write(str(len(data)).encode())
        self.writer.write(b'\n')
        self.writer.write(data)
        await self.writer.drain()
