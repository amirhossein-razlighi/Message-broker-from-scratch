import asyncio
import hashlib
import random

from broker import Broker


def hash_function(key):
    return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)


class ZooKeeper(Broker):
    def __init__(self):
        super().__init__()
        self._broker_list = []
        self._partitions = {}
        self._broker_partitions = {}

    def add_broker(self, broker, partition, replica=None):
        position = hash_function(broker.id)
        self._broker_list.append((position, broker))

        if partition not in self._partitions:
            self._partitions[partition] = []

        self._partitions[partition].append(broker)
        self._broker_partitions[broker] = partition

        self._broker_list.sort()
        print(f"Broker {broker.id} added at position {position} in partition {partition}")

        if replica is not None:
            self.add_replica(broker)

    def add_replica(self, broker):
        new_broker = Broker()

        original_partition = self._broker_partitions[broker]

        other_partitions = [p for p in self._partitions if p != original_partition]

        if not other_partitions:
            print("No other partitions available to add the replica.")
            return

        partition = random.choice(other_partitions)

        if partition not in self._partitions:
            self._partitions[partition] = []

        self._partitions[partition].append(new_broker)
        self._broker_partitions[new_broker] = partition

        self.add_broker(new_broker, partition)

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


"""
if __name__ == '__main__':
    zookeeper = ZooKeeper()
    broker1 = Broker()
    broker2 = Broker()
    zookeeper.add_broker(broker1, 'partition1')
    zookeeper.add_replica('partition2')
    zookeeper.add_broker(broker2, 'partition3')
    asyncio.run(zookeeper.main())
"""
# working test
if __name__ == '__main__':
    zookeeper = ZooKeeper()

    # Add multiple brokers to different partitions
    brokers = [Broker() for _ in range(5)]
    for i, broker in enumerate(brokers):
        partition = f'partition{i}'
        zookeeper.add_broker(broker, partition)

    # Add a replica for each broker
    for broker in brokers:
        zookeeper.add_replica(broker)

    # Print out the brokers list
    print("Brokers List:")
    for position, broker in zookeeper._broker_list:
        print(f"Broker ID: {broker.id}, Position: {position}")

    # Print out the partitions dictionary
    print("\nPartitions Dictionary:")
    for partition, brokers in zookeeper._partitions.items():
        print(f"Partition: {partition}")
        for broker in brokers:
            print(f"Broker ID: {broker.id}")
