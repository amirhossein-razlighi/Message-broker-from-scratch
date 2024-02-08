import asyncio
import hashlib
import pickle
import random
import socket

from broker import Broker
import fastapi
import asyncio

from server.Replica import Replica
from server.pqueue import Pqueue


def hash_function(key):
    return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)


class ZooKeeper(Broker):

    def __init__(self, host, port):
            super().__init__(host, port)
            self._broker_list = []
            self._partitions = {}
            self._broker_partitions = {}
            self._brokers = {}  # New dictionary to map broker_id to broker

    def add_broker(self, broker, partition, replica=None):
        message = {'type': 'add_broker', 'partition': partition, 'replica': replica}
        response = self.send_message(broker, message)
        print(response)
        self._brokers[broker.id] = broker  # Add the broker to the _brokers dictionary

    def handle_message(self, message):
        if message['type'] == 'add_broker':
            broker_id = message['broker_id']
            partition = message['partition']
            replica = message['replica']

            position = hash_function(broker_id)
            self._broker_list.append((position, broker_id))

            if partition not in self._partitions:
                self._partitions[partition] = []

            self._partitions[partition].append(broker_id)
            self._broker_partitions[broker_id] = partition

            self._broker_list.sort()
            print(f"Broker {broker_id} added at position {position} in partition {partition}")

            if replica is not None:
                self.add_replica(broker_id)

            return 'Broker added successfully'

        if message['type'] == 'add_replica':
            broker_id = message['broker_id']
            broker = self._brokers[broker_id]  # Get the broker from the _brokers dictionary

            new_broker = Replica(broker.host, broker.port)

            original_partition = self._broker_partitions[broker_id]

            other_partitions = [p for p in self._partitions if p != original_partition]

            if not other_partitions:
                print("No other partitions available to add the replica.")
                return

            partition = random.choice(other_partitions)

            if partition not in self._partitions:
                self._partitions[partition] = []

            self._partitions[partition].append(new_broker)
            self._broker_partitions[new_broker] = partition

            # Create a priority queue for the replica
            new_broker._pqueues[partition] = Pqueue(partition, is_replica=True)

            broker.register(new_broker)  # register new_broker as an observer to the original broker

            self.add_broker(new_broker, partition)

            return 'Replica added successfully'

    def send_message(self, broker, message):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((broker.host, broker.port))
            s.sendall(pickle.dumps(message))
            data = s.recv(1024)
        return pickle.loads(data)

    def add_replica(self, broker):
        message = {'type': 'add_replica'}
        response = self.send_message(broker, message)
        print(response)

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
to check single process use this , they worked the single process version.
    
    These are just some tests for checking the validation
    Pass
    if __name__ == '__main__':
        zookeeper = ZooKeeper()
        broker1 = Broker()
        broker2 = Broker()
        zookeeper.add_broker(broker1, 'partition1')
        zookeeper.add_replica('partition2')
        zookeeper.add_broker(broker2, 'partition3')
        asyncio.run(zookeeper.main())
    
    Pass
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
    
    # assert true check
    
    if __name__ == '__main__':
        host = "localhost"
        port = 8000
    
        # Create a ZooKeeper
        zookeeper = ZooKeeper(host, port)
    
        # Create some brokers and add them to different partitions
        brokers = [Broker(host, port + i) for i in range(3)]
        for i, broker in enumerate(brokers):
            zookeeper.add_broker(broker, partition=f"partition_{i}")
            print(broker)  # Print the state of the broker
    
        # Create some replicas of the brokers
        for broker in brokers:
            zookeeper.add_replica(broker)
            print(broker)  # Print the state of the broker
    
    
        for part_no, brokers_in_partition in zookeeper._partitions.items():
            for broker in brokers_in_partition:
                if not broker._is_replica:  # Only write to original brokers, not replicas
                    message = "test_message"
                    if part_no not in broker._pqueues:
                        broker._pqueues[part_no] = Pqueue(part_no, is_replica=False)
                    ret_val = broker._pqueues[part_no].write_new_message(message)
                    print(f"Return value of write_new_message: {ret_val}")
    
                    # Mark the message as read in the original broker
                    ret_val = broker._pqueues[part_no].mark_as_read()
                    print(f"Return value of mark_as_read: {ret_val}")
    
                    # Notify the ZooKeeper
                    ret_val = broker.notify(part_no, message)
                    print(f"Return value of notify: {ret_val}")
    
                    # Check if the replicas are updated correctly
                    for observer in broker._observers:
                        if part_no in observer._pqueues:
                            assert observer._pqueues[
                                       part_no].read() == message, f"Replica {observer.id} was not updated correctly!"
                        else:
                            print(f"Replica {observer.id} does not have a priority queue for {part_no}")
                            
"""
