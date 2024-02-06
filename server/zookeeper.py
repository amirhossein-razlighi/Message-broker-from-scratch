from broker import Broker

class ZooKeeper(Broker):
    def __init__(self):
        super().__init__()
        self._broker_list = []

    def add_broker(self, broker):
        self._broker_list.append(broker)

    def remove_broker(self, broker):
        self._broker_list.remove(broker)

    def get_broker(self):
        return self._broker_list[0]

    async def main(self):
        server = await asyncio.start_server(self.handle_client, self._host, self._port)
        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')

        async with server:
            await server.serve_forever()

if __name__ == '__main__':
    zookeeper = ZooKeeper()
    broker = Broker()
    zookeeper.add_broker(broker)
    asyncio.run(zookeeper.main())
    