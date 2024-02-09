import asyncio
import hashlib
import pickle
import random
import socket

from broker import Broker
import asyncio

from server.Replica import Replica
from server.pqueue import Pqueue


def hash_function(key):
    return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)


class ZooKeeper(Broker):
    def __init__(self, host, port):
            super().__init__(host, port)
            self._broker_list = []
            self._partitions = {}
            self._broker_partitions = {}
            self._brokers = {}  # New dictionary to map broker_id to broker
            self._global_subscribers = []
            self._current_broker_index = 0  # Used for round-robin

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
                
            self._broker_list.sort()
            print(
                f"Broker {broker.id} added at position {position} in partition {partition}"
            )

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

        # changes
        if broker.id not in self.brokers:
            self.brokers[broker.id] = broker
            print(f"Broker {broker.id} added.")
        else:
            print(f"Error: Broker {broker.id} already exists.")

    def add_replica(self, broker):
        message = {'type': 'add_replica'}
        response = self.send_message(broker, message)
        print(response)

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

    @staticmethod
    def consume():
        for broker_id in brokers:
            if not brokers[broker_id].is_empty:
                return brokers[broker_id]

    async def main(self):
        server = await asyncio.start_server(self.handle_client, self._host, self._port)
        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')
        
    def choose_broker_for_subscription(self, subscriber):
        broker = self._broker_list[self._current_broker_index][1]
        broker_id = broker.id
        self._current_broker_index = (self._current_broker_index + 1) % len(
            self._broker_list
        )
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((broker.host, broker.socket_port))
            s.sendall(
                json.dumps(
                    {
                        "type": "SUBSCRIBE",
                        "subscriber": subscriber,
                        "broker_id": broker_id,
                    }
                ).encode()
            )
            data = s.recv(1024)
            print("Received", repr(data))
            if repr(data) == SOCKET_STATUS.WRITE_SUCCESS.value:
                self._logger.info(
                    f"Subscriber {subscriber} subscribed to broker {broker_id} with host {broker.host} and socket_port {broker.socket_port}"
                )
                self._global_subscribers.append(subscriber)
                return STATUS.SUCCESS
            else:
                self._logger.error(
                    f"Subscriber {subscriber} failed to subscribe to broker {broker_id} with host {broker.host} and socket_port {broker.socket_port}"
                )
                return STATUS.ERROR

    def get_broker_for_partition(self, partition):
        brokers = self._partitions[partition]
        return random.choice(brokers)

    def _push_to_broker(self, broker, json_dict):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((broker.host, broker.socket_port))
            s.sendall(json.dumps(json_dict).encode())
            data = s.recv(1024)
            print("Received", repr(data))
            if repr(data) == SOCKET_STATUS.WRITE_SUCCESS.value:
                return STATUS.SUCCESS
            else:
                return STATUS.ERROR

    # Overriding the Broker's "handle_client" method
    async def handle_client(self, reader, writer):
        data = await reader.read(100)
        message = data.decode()
        addr = writer.get_extra_info("peername")
        print(f"Received {message} from {addr}")

        json_dict = self.extract_message(message)
        if json_dict["type"] == "PUSH":
            broker = self.get_broker_for_partition(json_dict["part_no"])
            status = self._push_to_broker(broker, json_dict)
            if status == STATUS.SUCCESS:
                writer.write(SOCKET_STATUS.WRITE_SUCCESS.value.encode())
            else:
                writer.write(SOCKET_STATUS.WRITE_ERROR.value.encode())
            await writer.drain()
        elif json_dict["type"] == "PULL":
            part_no = json_dict["part_no"]
            if part_no not in self._pqueues:
                # error no such queue
                return "Invalid"
            message = self._read(part_no)
        elif json_dict["type"] == "SUBSCRIBE":
            status = self.choose_broker_for_subscription(json_dict["subscriber"])
            if status == STATUS.SUCCESS:
                writer.write(SOCKET_STATUS.WRITE_SUCCESS.value.encode())
            else:
                writer.write(SOCKET_STATUS.WRITE_ERROR.value.encode())
            await writer.drain()
        else:
            writer.write("Invalid".encode())

        self._logger.info(f"Closing the connection")
        writer.close()
        # await writer.drain()
        # print("Close the connection")
        # writer.close()


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
