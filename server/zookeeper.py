import asyncio
import hashlib
import random
import socket
import time
import threading

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

    def update_broker_status(self, broker_id, status):
        if broker_id in self.brokers:
            self.brokers[broker_id].is_up = status
        else:
            print(f"Error: Broker {broker_id} not found.")

    def get_active_brokers(self):
        active_brokers = [broker_id for broker_id, data in self.brokers.items() if data.is_up == 1]
        return active_brokers

    def start_broker(self, broker_id):
        self.update_broker_status(broker_id, True)
        print(f"Broker {broker.id} started.")

    def stop_broker(self, broker_id):
        self.update_broker_status(broker_id, False)
        print(f"Broker {broker.id} stopped.")

    def consume(self):
        for broker_id in self.brokers:
            if not brokers[broker_id].is_empty and brokers[broker_id].is_up:
                return brokers[broker_id]

    def send_heartbeat(self, broker_id):
        broker_address = (self.brokers[broker_id].host, self.brokers[broker_id].ping_port)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(3)
                s.connect(broker_address)
                s.sendall(b"PING")
                self.start_broker(broker_id)
                print(f"Sent heartbeat to {broker_address}")
        except socket.timeout:
            self.stop_broker(broker_id)
            print(f"Timeout sending heartbeat to {broker_address}")
        except Exception as e:
            self.stop_broker(broker_id)
            print(f"Error sending heartbeat to {broker_address}: {e}")

    def health_check_thread(self):
        while True:
            for broker_id in self.brokers:
                self.send_heartbeat(broker_id)

            time.sleep(5)

    async def main(self):
        server = await asyncio.start_server(self.handle_client, self._host, self._port)
        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')

        health_check_thread = threading.Thread(target=self.health_check_thread, daemon=True)
        health_check_thread.start()

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


