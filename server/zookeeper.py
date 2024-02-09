import asyncio
import hashlib
import random

from broker import Broker
import asyncio


def hash_function(key):
    return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)


class ZooKeeper(Broker):
    def __init__(self, brokers_list=None):
        super().__init__()
        self._broker_list = []
        self._partitions = {}
        self._broker_partitions = {}
        self.brokers = {broker.id: broker for broker in brokers_list} if brokers_list else {}
        self._global_subscribers = []
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
