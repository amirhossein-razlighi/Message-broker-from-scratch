import asyncio
import hashlib
import random

from broker import Broker
import fastapi
import asyncio


def hash_function(key):
    return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)


class ZooKeeper(Broker):
    def __init__(self):
        super().__init__()
        self._broker_list = []
        self._partitions = {}
        self._broker_partitions = {}
        self.global_subscribers = []
        self._current_broker_index = 0  # Used for round-robin

    def add_broker(self, broker, partition, replica=None):
        position = hash_function(broker.id)
        self._broker_list.append((position, broker))

        if partition not in self._partitions:
            self._partitions[partition] = []

        self._partitions[partition].append(broker)
        self._broker_partitions[broker] = partition

        self._broker_list.sort()
        print(
            f"Broker {broker.id} added at position {position} in partition {partition}"
        )

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

    def choose_broker_for_subscription(self, subscriber):
        broker = self._broker_list[self._current_broker_index][1]
        broker_id = broker.id
        self._current_broker_index = (self._current_broker_index + 1) % len(
            self._broker_list
        )
        broker.broker_subscribers.append(subscriber)
        print(
            f"Subscriber {subscriber} assigned to broker {broker.host}: {broker.socket_port}"
        )

    # This can be removed, due to inheritance from Broker
    async def main(self):
        server = await asyncio.start_server(self.handle_client, self._host, self._port)
        addr = server.sockets[0].getsockname()
        print(f"Serving on {addr}")

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
if __name__ == "__main__":
    zookeeper = ZooKeeper()

    brokers = [Broker() for _ in range(5)]
    for i, broker in enumerate(brokers):
        partition = f"partition{i}"
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
