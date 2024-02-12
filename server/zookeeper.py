import asyncio
import json
import os, sys

from status import STATUS, SOCKET_STATUS

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
import hashlib
import pickle
import random
import socket
import time
import threading
import argparse
import fastapi
import uvicorn

from broker import Broker
import asyncio

from pqueue import Pqueue

from metrics import *


class ZooKeeper(Broker):
    def __init__(self, host, socket_port, http_port, ping_port):
        super().__init__(host, socket_port, http_port, ping_port)
        self.is_master = False # check if the current zookeeper is master zookeeper
        self.addresses = {} # dictionary of brokers_idf to the tuple of (broker_host_ip, broker_socket_port)
        self._partitions = {} # dict: partition_number -> active broker_id
        self._partitions_replica = {} # dict: partition_number -> replica broker_id
        # check if the broker is up or down
        self.is_up = {}
        # check if the broker is empty or not
        self.is_empty = {}
        self.ping_addresses = {}
        self._partitions_broker={}  # dict: partition_number -> Broker broker_id


        self._broker_list = [] # (partition_id, broker_id)
        self._broker_partitions = {}
        self._brokers = {}  # New dictionary to map broker_id to broker
        self._current_broker_index = 0  # Used for round-robin

        self._global_subscribers = []
        self.is_zookeeper_nominee = True

        self.first_replica = []

    # async def handle_broker(self, reader, writer):
    #     # Receive broker's information
    #     broker_info = (await reader.read(1024)).decode()
    #     host, port, ping_port, idf = broker_info.split(":")
    #     self.addresses[idf]=(host, int(port))
    #     self.addresses[idf]=(host, int(ping_port))
    #     print(f"Broker at {host}:{port} added to the network.")
    #     writer.close()
    #
    # async def start(self):
    #     # Create server
    #     server = await asyncio.start_server(
    #         self.handle_broker, self.host, self.port
    #     )
    #
    #     # Print server information
    #     addr = server.sockets[0].getsockname()
    #     print(f"Leader Broker listening on {addr}")
    #
    #     # Serve requests until interrupted
    #     async with server:
    #         await server.serve_forever()

    def remove_broker(self, broker):
        self._broker_list.remove(broker)

        # changes
        if broker.id in self._brokers:
            self._brokers.pop(broker.id)
            print(f"Broker {broker.id} removed.")
        else:
            print(f"Error: Broker {broker.id} not found.")

    def get_broker(self):
        return self._broker_list[0]

    def update_broker_status(self, broker_id, status):
        if broker_id in self.is_up:
            self.is_up[broker_id] = status
        else:
            print(f"Error: Broker {broker_id} not found.")

    def hash_function(self, key):
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16) % (len(self._brokers)+1)

    def get_active_brokers(self):
        active_brokers = [broker_id for broker_id, data in self.is_up.items() if data == 1]
        return active_brokers

    def start_broker(self, broker_id):
        self.update_broker_status(broker_id, 1)
        print(f"Broker {broker_id} started.")

    def stop_broker(self, broker_id):
        self.update_broker_status(broker_id, 0)
        print(f"Broker {broker_id} stopped.")

    def consume(self, command='pull'):
        if command == 'sub':
            items = list(self.is_up.items())
            random.shuffle(items)
            for broker_id, is_active in items:
                if is_active == 1:
                    return broker_id
        else:

            for broker_id in self.is_up:
                if self.is_empty[broker_id] == 0 and self.is_up[broker_id] == 1:
                    return broker_id
        return None

    def send_heartbeat(self, broker_id):
        broker_address = self.ping_addresses[broker_id]
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
            for broker_address in self.ping_addresses:
                print("Checking broker", broker_address)
                self.send_heartbeat(broker_address)
            time.sleep(5)

    def run(self, host=None, http_port=None, socket_port=None):
        if host is None:
            host = self._host
        if http_port is None:
            http_port = self._http_port
        if socket_port is None:
            socket_port = self._socket_port

        app = fastapi.FastAPI(port=int(http_port), host=host)

        app.add_api_route("/", self.read_root, methods=["GET"])
        app.add_api_route("/zookeeper", self.get_zookeeper, methods=["GET"])
        app.add_api_route("/metrics", self.gen_metrics, methods=["GET"])

        socket_thread = threading.Thread(
            target=asyncio.run, args=(self.socket_thread(),)
        )
        socket_thread.start()

        http_thread = threading.Thread(
            target=asyncio.run, args=(uvicorn.run(app, host=host, port=int(http_port)),)
        )
        http_thread.start()

        health_check_thread = threading.Thread(target=self.health_check_thread, daemon=True)
        health_check_thread.start()

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

    # def get_broker_for_partition(self, partition):
    #     brokers = self._partitions[partition]
    #     return random.choice(brokers)

    def hash_message_key(self, message_key):
        hasher = hashlib.sha256()
        hasher.update(message_key.encode('utf-8'))
        hashed_value = int.from_bytes(hasher.digest(), byteorder='big')
        num_active = 0
        active_brokers = []
        for broker_id, state in self.is_up.items():
            if state == 1:
                active_brokers.append(broker_id)
        partition_index = hashed_value % len(active_brokers)
        res = active_brokers[partition_index]
        return res

    def _push(self, json_dict):
        broker_id = self.hash_message_key(json_dict["key"])
        status = self._push_pull_broker(broker_id, json_dict)
        print(f"STATUS in push: {status}")
        if status == STATUS.SUCCESS:
            self.is_empty[broker_id] = 0
            return STATUS.SUCCESS
        else:
            return STATUS.ERROR

    def _push_pull_broker(self, broker_id, json_dict):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print(f"Connecting to {self.addresses[broker_id]}")
            print(f"Sending {json.dumps(json_dict)}")
            try:
                s.connect(self.addresses[broker_id])
            except Exception as e:
                print(f"Error connecting to {self.addresses[broker_id]}: {e}")
                return STATUS.ERROR
            print("Connected")
            s.sendall(json.dumps(json_dict).encode())
            data = s.recv(1024).decode()
            data = data.strip()
            if data == str(SOCKET_STATUS.WRITE_SUCCESS.value):
                return STATUS.SUCCESS
            else:
                return STATUS.ERROR
    def _get_next_broker(self, current_broker_id):
        # Get the next broker in the list that is not the current broker
        for broker_id in self._brokers:
            if broker_id != current_broker_id:
                return broker_id
        return None
    # Overriding the Broker's "handle_client" method
    async def handle_client(self, reader, writer):
        while True:
            data = await reader.read(100)
            message = data.decode()
            host = writer.get_extra_info('peername')[0]
            port = writer.get_extra_info('peername')[1]
            print(f"Received {message} from {(host, port)}")
            if message.startswith("initiate"):
                a, b, port, ping_port, idf = message.split(":")
                host = socket.gethostbyname(host)
                self.addresses[idf] = (host, int(port))
                self.ping_addresses[idf] = (host, int(ping_port))

                self.is_up[idf] = 1
                self.is_empty[idf] = 1

                print(f"Broker at {host}:{port} added to the network.")
                broker_id = idf
                partition = self.hash_function(broker_id)
                self._broker_list.append((partition, broker_id))
                self._brokers[broker_id] = Broker(host, 8000, 8888, ping_port)



                self._broker_list.sort()
                print(
                    f"partition {partition} added in broker {broker_id}"
                )
                if partition not in self._partitions_broker:
                    self._partitions_broker[partition] = []
                self._partitions_broker[partition] = broker_id

                # replica
                other_broker_id=self._get_next_broker(broker_id)
                if other_broker_id ==None:
                    my_tuple = (broker_id, partition)
                    self.first_replica.append(my_tuple)
                if len(self._brokers)>2:
                    if partition not in self._partitions_replica:
                        self._partitions_replica[partition] = []
                    self._partitions_replica[partition] = other_broker_id
                    other_Broker=self._brokers[other_broker_id]
                    other_Broker._create_pqueue(partition,is_replica=True)
                    print("Replica Created successfully")
                if len(self._brokers)==2:
                    if partition not in self._partitions_replica:
                        self._partitions_replica[partition] = []
                    self._partitions_replica[partition] = other_broker_id
                    other_Broker=self._brokers[other_broker_id]
                    other_Broker._create_pqueue(partition,is_replica=True)
                    my_tuple = self.first_replica[0]
                    first_broker_id = my_tuple[0]
                    first_partition = my_tuple[1]
                    if first_partition not in self._partitions_replica:
                        self._partitions_replica[first_partition] = []
                    self._partitions_replica[first_partition] = first_broker_id
                    other_Broker=self._brokers[first_broker_id]
                    other_Broker._create_pqueue(first_partition,is_replica=True)
                    print("Replica Created successfully")
                    print("Replica Created successfully")




            else:
                addr = writer.get_extra_info("peername")
                print(f"Received {message} from {addr}")

                json_dict = self.extract_message(message)
                if json_dict["type"] == "PUSH":
                    status = self._push(json_dict)
                    status = STATUS.SUCCESS
                    print(f"Status: {status}")
                    if status == STATUS.SUCCESS:
                        response = str(SOCKET_STATUS.WRITE_SUCCESS.value)
                        writer.write(response.encode())
                    else:
                        writer.write(SOCKET_STATUS.WRITE_ERROR.value.encode())
                    print(f"Sent {SOCKET_STATUS.WRITE_SUCCESS.value}")
                    await writer.drain()
                elif json_dict["type"] == "PULL":
                    broker_id = self.consume()
                    if broker_id is not None:
                        writer.write(','.join(str(x) for x in self.addresses[broker_id]).encode())
                    else:
                        writer.write('Brokers are empty'.encode())
                    await writer.drain()

                elif json_dict["type"] == "SUBSCRIBE":
                    broker_id = self.consume('sub')
                    if broker_id is not None:
                        text = ','.join(str(x) for x in self.addresses[broker_id]) + ',' + broker_id
                        writer.write(text.encode())
                    else:
                        writer.write('There is not broker'.encode())
                    await writer.drain()
                else:
                    break

        self._logger.info(f"Closing the connection")
        writer.close()
        await writer.wait_closed()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument(
        "--socket_port", default=8000, help="Port to bind socket connection to"
    )
    parser.add_argument(
        "--http_port", default=8888, help="Port to bind https connection to"
    )
    parser.add_argument(
        "--ping_port", default=7500, help="Port to bind ping connection to"
    )
    args = parser.parse_args()
    print(args.host, args.socket_port, args.http_port, args.ping_port)

    zookeeper = ZooKeeper(args.host, args.socket_port, args.http_port, args.ping_port)
    zookeeper.run(args.host, args.http_port, args.socket_port)
