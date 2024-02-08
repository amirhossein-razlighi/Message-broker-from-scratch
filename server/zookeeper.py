import asyncio
import hashlib
import random

from broker import Broker
import asyncio


def hash_function(key):
    return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)


class ZooKeeper(Broker):
    def __init__(self, brokers_list=None):
        super().__init__()
        self._broker_list = []
        self._partitions = {}
        self._broker_partitions = {}
        self.brokers = {broker.id: broker for broker in brokers_list} if brokers_list else {}

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

        # changes
        if broker.id not in self.brokers:
            self.brokers[broker.id] = broker
            print(f"Broker {broker.id} added.")
        else:
            print(f"Error: Broker {broker.id} already exists.")

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

        # changes
        if broker.id in self.brokers:
            self.brokers.pop(broker.id)
            print(f"Broker {broker.id} removed.")
        else:
            print(f"Error: Broker {broker.id} not found.")

    def get_broker(self):
        return self._broker_list[0]

    def update_broker_status(self, broker, status):
        if broker.id in self.brokers:
            self.brokers[broker.id].is_up = status
        else:
            print(f"Error: Broker {broker.id} not found.")

    def get_active_brokers(self):
        active_brokers = [broker_id for broker_id, data in self.brokers.items() if data.is_up == 1]
        return active_brokers

    def start_broker(self, broker):
        self.update_broker_status(broker.id, 1)
        print(f"Broker {broker.id} started.")

    def stop_broker(self, broker):
        self.update_broker_status(broker.id, 0)
        print(f"Broker {broker.id} stopped.")

    def consume(self):
        for broker in brokers:
            if not broker.is_empty:
                return broker

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
if __name__ == '__main__':
    zookeeper = ZooKeeper()

    brokers = [Broker() for _ in range(5)]
    for i, broker in enumerate(brokers):
        partition = f'partition{i}'
        zookeeper.add_broker(broker, partition)

    for broker in brokers:
        zookeeper.add_replica(broker)

    print("Brokers List:")
    for position, broker in zookeeper._broker_list:
        print(f"Broker ID: {broker.id}, Position: {position}")

    print("\nPartitions Dictionary:")
    for partition, brokers in zookeeper._partitions.items():
        print(f"Partition: {partition}")
        for broker in brokers:
            print(f"Broker ID: {broker.id}")
