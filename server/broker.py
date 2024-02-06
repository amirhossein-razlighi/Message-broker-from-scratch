import queue
import asyncio
import socket

class Broker:
    def __init__(self):
        self._queue = queue.Queue()

    def publish(self, data):
        self._queue.put(data)

    def consume(self):
        return self._queue.get()

    async def handle_client(self, reader, writer):
        data = await reader.read(100)
        message = data.decode()
        addr = writer.get_extra_info('peername')
        print(f"Received {message!r} from {addr!r}")
        
    async def main(self):
        server = await asyncio.start_server(
            self.handle_client, '127.0.0.1', 8888)

        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')

        async with server:
            await server.serve_forever()

if __name__ == '__main__':
  broker = Broker()
  asyncio.run(broker.main())