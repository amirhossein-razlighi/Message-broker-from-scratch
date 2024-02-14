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
import threading
import argparse
import fastapi
import uvicorn

from broker import Broker
import asyncio

from metrics import *


class ZooKeeper(Broker):
    def __init__(self, host, socket_port, http_port, ping_port):
        super().__init__(host, socket_port, http_port, ping_port)
        self.is_master = False  # check if the current zookeeper is master zookeeper
        self.addresses = (
            {}
        )  # dictionary of brokers_id to the tuple of (broker_host_ip, broker_socket_port)
        self._partitions = {}  # dict: partition_number -> active broker_id
        self._partitions_replica = {}  # dict: partition_number -> replica broker_id
        # check if the broker_id is up (1) or down (0)
        self.is_up = {}
        # check if the broker is empty or not , dict: broker_id -> is_up (1) or not(0)
        self.is_empty = {}
        self.ping_addresses = {}
        self._last_assigned_partition = 0  # Used to assign partitions to brokers

        self._global_subscribers = []
        self.is_zookeeper_nominee = True

    def remove_broker(self, broker):
        pass

    def update_broker_status(self, broker_id, status):
        if broker_id in self.is_up:
            self.is_up[broker_id] = status
        else:
            print(f"Error: Broker {broker_id} not found.")

    def get_active_brokers(self):
        active_brokers = [
            broker_id for broker_id, data in self.is_up.items() if data == 1
        ]
        return active_brokers

    def start_broker(self, broker_id):
        self.update_broker_status(broker_id, 1)
        print(f"Broker {broker_id} started.")

    async def partitions_move(self, broker_id):
        # make active partitions of broker_id active in it's replica
        for part in self._partitions:
            if self._partitions[part] == broker_id:
                message = {
                    "type": "PARTITION_ACTIVATE",
                    "partition": part
                }
                broker_id_replica_part = self._partitions_replica[part]
                address_replica_broker = self.addresses[broker_id_replica_part]
                await self.send_message_to_broker(
                    address_replica_broker[0],
                    address_replica_broker[1],
                    message
                )
                self._partitions_replica[part] = None
                self._partitions[part] = broker_id_replica_part

        # notify main partition brokers that their replica is down
        for part in self._partitions_replica:
            if self._partitions_replica[part] == broker_id:
                message = {
                    "type": "REPLICA_DOWN",
                    "partition": part
                }
                broker_id_main_part = self._partitions[part]
                address_main_broker = self.addresses[broker_id_main_part]
                await self.send_message_to_broker(
                    address_main_broker[0],
                    address_main_broker[1],
                    message
                )
                self._partitions_replica[part] = None


    async def stop_broker(self, broker_id):
        self.update_broker_status(broker_id, 0)
        self.is_empty.pop(broker_id)
        await self.partitions_move(broker_id)
        self.addresses.pop(broker_id)
        self.ping_addresses.pop(broker_id)
        self._logger.info(f"Broker {broker_id} removed.")

    def consume(self, command="pull"):
        if command == "sub":
            items = list(self._partitions.items())
            random.shuffle(items)
            for partition_number, broker_id in items:
                if self.is_empty[broker_id] == 0:
                    return broker_id, partition_number
        else:
            partition_number = random.choice(list(self._partitions.keys()))
            return self._partitions[partition_number], partition_number
        return None

    def send_heartbeat(self, broker_id):
        broker_address = self.ping_addresses[broker_id][0]
        ping_resp = os.system(f"ping -c 1 {broker_address} > /dev/null 2>&1")
        if ping_resp == 0:
            return True  # Broker is up
        else:
            return False

    async def health_check_thread(self):
        self._logger.info("health check is starting")
        self._logger.info(f"Broker list: {self.ping_addresses}")
        while True:
            for broker_id in self.ping_addresses:
                broker_is_up = self.send_heartbeat(broker_id)
                self._logger.info(f"Broker {self.ping_addresses[broker_id][0]} is being pinged")
                if not broker_is_up:
                    # try 3 more times with distance if broker is not up, call stop_broker
                    for _ in range(3):
                        broker_is_up = self.send_heartbeat(broker_id)
                        time.sleep(0.2)
                    if not broker_is_up:
                        self._logger.info(f"Broker {self.ping_addresses[broker_id][0]} is down")
                        await self.stop_broker(broker_id)
                        break

            time.sleep(3)

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

        health_check_thread = threading.Thread(
            target=asyncio.run, args=(self.health_check_thread(), ), daemon=True
        )
        health_check_thread.start()

        http_thread = threading.Thread(
            target=asyncio.run, args=(uvicorn.run(app, host=host, port=int(http_port)),)
        )
        http_thread.start()

        health_check_thread = threading.Thread(
            target=self.health_check_thread, daemon=True
        )
        health_check_thread.start()

    def hash_message_key(self, message_key):
        hasher = hashlib.sha256()
        hasher.update(message_key.encode("utf-8"))
        hashed_value = int.from_bytes(hasher.digest(), byteorder="big")
        part_no = hashed_value % len(self._partitions) + 1
        return part_no

    def _push(self, json_dict):
        part_no = self.hash_message_key(json_dict["key"])
        json_dict["part_no"] = part_no
        print(f"Partition number: {part_no}")
        print(self._partitions)
        broker_id = self._partitions[part_no]
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

    # Overriding the Broker's "handle_client" method
    async def handle_client(self, reader, writer):
        while True:
            data = await reader.read(100)
            message = data.decode()
            host = writer.get_extra_info("peername")[0]
            port = writer.get_extra_info("peername")[1]
            print(f"Received {message} from {(host, port)}")
            print(message)
            if message.startswith("initiate"):
                a, b, port, ping_port, idf = message.split(":")
                port = int(port)
                host = socket.gethostbyname(host)
                self._logger.info(f"host is {host}")
                self.addresses[idf] = (host, int(port))
                self.ping_addresses[idf] = (host, int(ping_port))

                self.is_up[idf] = 1
                self.is_empty[idf] = 1

                print(f"Broker at {host}:{port} added to the network.")
                broker_id = idf
                partition = self._last_assigned_partition
                self._last_assigned_partition += 1

                # choose a broker to take the replica of the partition
                replica_broker_id = None
                if (
                    len(self.addresses) == 1
                ):  # the first partition is redundant and would not exist
                    continue

                self._logger.info(
                    f"partition {partition} added in broker {broker_id} with ip {host}"
                )

                if len(self.addresses) > 1:
                    lst = list(self.addresses.keys())
                    lst.remove(broker_id)
                    replica_broker_id = random.choice(lst)
                    self._partitions_replica[partition] = replica_broker_id
                    self._logger.info(
                        f"partition {partition} replica is in broker {replica_broker_id} and main broker is {broker_id}"
                    )
                else:
                    writer.write("No other broker to take the replica".encode())

                # notify the broker to take the partition
                message = {
                    "type": "ADD_PARTITION",
                    "partition": partition,
                    "replica_address": self.addresses[replica_broker_id],
                }
                await self.send_message_to_broker(
                    host, int(port), message
                )

                # notify the replica broker to take the replica
                message = {"type": "ADD_REPLICA_PARTITION", "partition": partition}
                await self.send_message_to_broker(
                    self.addresses[replica_broker_id][0],
                    self.addresses[replica_broker_id][1],
                    message,
                )

                self._partitions[partition] = broker_id
                self._partitions_replica[partition] = replica_broker_id

            else:
                addr = writer.get_extra_info("peername")
                print(f"Received {message} from {addr}")

                json_dict = self.extract_message(message)
                if json_dict["type"] == "PUSH":
                    status = self._push(json_dict)
                    print(f"Status: {status}")
                    if status == STATUS.SUCCESS:
                        response = str(SOCKET_STATUS.WRITE_SUCCESS.value) #+ "\n" #JAVA
                        writer.write(response.encode())
                    else:
                        writer.write(SOCKET_STATUS.WRITE_ERROR.value.encode())
                    print(f"Sent {SOCKET_STATUS.WRITE_SUCCESS.value}")
                    await writer.drain()
                elif json_dict["type"] == "PULL":
                    broker_id, partition_number = self.consume()
                    print(f"Broker id: {broker_id}, Partition number: {partition_number}")
                    if broker_id is not None:
                        response = ",".join(str(x) for x in self.addresses[broker_id])
                        response += "," + str(partition_number) #+ "\n" #JAVA
                        writer.write(response.encode())
                    else:
                        writer.write("Brokers are empty".encode())
                    await writer.drain()

                elif json_dict["type"] == "SUBSCRIBE":
                    broker_id, partition_number = self.consume("sub")
                    if broker_id is not None:
                        text = (
                            ",".join(str(x) for x in self.addresses[broker_id])
                            + ","
                            + str(partition_number)
                        ) #+ "\n" #JAVA
                        writer.write(text.encode())
                        print(f"SUBSCRIBE SENT TO CLIENT")
                    else:
                        writer.write("There is not broker".encode())
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
