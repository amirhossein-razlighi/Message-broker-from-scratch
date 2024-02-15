import asyncio
import os
import queue
import socket
import json
import fastapi
import argparse
import uvicorn
import threading
import uuid
from pqueue import Pqueue
import logging
from status import STATUS, SOCKET_STATUS
import random
import pickle

from metrics import *

app = None

uvicorn.logging.logging.basicConfig(level=uvicorn.logging.logging.DEBUG)


class Broker:
    def __init__(self, host, socket_port, http_port, ping_port):
        self.id = str(uuid.uuid4())
        # dict: part_no -> Pqueue
        self._pqueues = {}
        self._app = app
        self._host = host
        self._socket_port = int(socket_port)
        self._http_port = http_port
        self._zookeeper = {
            "host": os.getenv("ZOOKEEPER"),
            # "host": "localhost",
            "http_port": 8888,
            "socket_port": 8000,
            "ping_port": 7500,
        }
        self._broker_subscribers = {}
        self._logger = logging.getLogger(__name__)
        self._is_replica = False
        self.ping_port = ping_port
        self.is_zookeeper_nominee = False

    def __str__(self):
        return f"Broker(id={self.id}, pqueues={self._pqueues}, is_replica={self._is_replica})"

    def create_pqueue(self, part_no, is_replica, replica_address=None):
        self._pqueues[part_no] = Pqueue(part_no, is_replica, replica_address)

    def _read(self, part_no):
        message = self._pqueues[part_no].read()
        if message is not None:
            self._logger.info(f"Read message {message} from part_no {part_no}")
        else:
            message = "No message"
        return message

    def extract_message(self, message):
        try:
            json_dict = json.loads(message)
        except:
            json_dict = {"type": "Invalid"}
        return json_dict

    async def _subscribe_checker_and_notifier(self, writer):
        while True:
            try:
                # if writer is closed, remove the subscriber from the list
                if writer is not None and writer.is_closing():
                    self._broker_subscribers.pop((writer.get_extra_info("peername")[0], writer.get_extra_info("peername")[1]))
                    self._logger.info(f"Subscriber {writer.get_extra_info('peername')} removed")
                    print(f"Subscriber {writer.get_extra_info('peername')} removed")
                    return
                if not len(self._broker_subscribers) == 0:
                    selected_subscriber = random.choice(
                        list(self._broker_subscribers.keys())
                    )
                    message = self._read(self._broker_subscribers[selected_subscriber])
                    if message is None:
                        await asyncio.sleep(5)
                        continue
                    if message == "No message":
                        await asyncio.sleep(5)
                        continue
                    self._logger.info(f"Selected subscriber {selected_subscriber}")
                    self._logger.info(
                        f"Sending message {message} to subscriber {selected_subscriber}"
                    )
                    print(f"Sending message to subscriber {selected_subscriber}")
                    response = str(message) + "\n"
                    await asyncio.wait_for(writer.write(response.encode()), timeout=5)
                    # await writer.drain()
                    print(f"Message sent to subscriber {selected_subscriber}")
                    self._logger.info(
                        f"Message sent to subscriber {selected_subscriber}"
                    )
                    await self.update_replicas()
            except:
                continue
            await asyncio.sleep(5)

    def _push(self, json_dict):
        self._logger.info(f"Received PUSH message {json_dict}")
        push_request_metrics.inc()
        print(f"Received PUSH message {json_dict}")
        part_no = int(json_dict["part_no"])
        if part_no not in self._pqueues:
            self._logger.error(f"Invalid part_no {part_no}")
            print(f"Invalid part_no {part_no}")
            return STATUS.ERROR
        self._logger.info(f"Writing message {json_dict['value']} to part_no {part_no}")
        print(f"Writing message {json_dict['value']} to part_no {part_no}")
        key_value_json = {}
        key_value_json["key"] = json_dict["key"]
        key_value_json["value"] = json_dict["value"]
        message = self._pqueues[part_no].write_new_message(key_value_json)
        self._logger.info(f"Message written to part_no {part_no}")
        print(f"Message written to part_no {part_no}")
        self.is_empty = 0  # TODO ?

        return STATUS.SUCCESS

    def _pull(self, json_dict):
        self._logger.info(f"Received PULL message {json_dict}")
        pull_request_metrics.inc()
        try:
            part_no = int(json_dict["part_no"])
            self._pqueues[part_no]
        except:
            self._logger.info(f"Selected part_no {part_no}")

        self._logger.info(f"Reading message from part_no {part_no}")
        return self._read(part_no)

    def _subscribe(self, subscriber, broker_id=None, writer=None, part_no=None):
        subscriber = {"host": subscriber[0], "socket_port": subscriber[1]}
        self._logger.info(f"Received SUBSCRIBE message from {subscriber}")
        self._logger.info(
            f"Subscriber {subscriber} subscribed to broker {(subscriber['host'], subscriber['socket_port'])}"
        )
        self._broker_subscribers[(subscriber["host"], subscriber["socket_port"])] = int(
            part_no
        )
        asyncio.create_task(self._subscribe_checker_and_notifier(writer))
        return STATUS.SUCCESS

    async def send_message_to_broker(self, host_ip, port, message):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.connect((host_ip, port))
            client_socket.sendall(pickle.dumps(message))

    async def handle_broker_message(self, reader, writer, addr, message):
        print(f"Received {message} from {addr} (Handle Broker Message)")
        if message["type"] == "ADD_PARTITION":
            replica_address = message["replica_address"]
            self.create_pqueue(message["partition"], False, replica_address)
            self._logger.info(f"Partition {message['partition']} added")
            writer.write((str(SOCKET_STATUS.PARTITION_SUCCESS.value) + "\n").encode())
            writer.close()
            return
        elif message["type"] == "ADD_REPLICA_PARTITION":
            self.create_pqueue(message["partition"], True)
            self._logger.info(f"Replica Partition {message['partition']} added")
            writer.write((str(SOCKET_STATUS.PARTITION_SUCCESS.value) + "\n").encode())
            writer.close()
            return

        elif message["type"] == "REPLICA_CONSISTENCY":
            part_no = message["partition"]
            queue_ = pickle.loads(message["queue"])
            self._pqueues[part_no].queue = queue_
            self._logger.info(f"Replica consistency updated for partition {part_no}")
            writer.write(
                (str(SOCKET_STATUS.REPLICA_CONSISTENCY_SUCCESS.value) + "\n").encode()
            )
            writer.close()
            return
        elif message['type'] == "PARTITION_ACTIVATE":
            part = message["partition"]
            self._pqueues[part].become("primary")
            self._logger.info(f"Partition {part} activated in {self.id}")
            return
        # if replica of the main partition which the main partition resides in this broker is down
        elif message['type'] == "REPLICA_DOWN":
            part = message["partition"]
            self._pqueues[part].remove_replica()
            self._logger.info(f"Partition {part} replica address is removed from primary broker {self.id}")
            return
        elif message['type'] == "MOVE_PARTITION_DATA":
            part = message["partition"]
            self._pqueues[part]
    
    async def update_replicas(self, push=False):
        for partition_number in self._pqueues.keys():
            if not self._pqueues[partition_number].is_replica:
                replica_addr = self._pqueues[partition_number].replica_address
                if replica_addr is not None:
                    print(f"Replica address: {replica_addr}")
                    host, port = replica_addr
                    host = socket.gethostbyname(host)
                    port = int(port)
                    message = {
                        "type": "REPLICA_CONSISTENCY",
                        "partition": partition_number,
                        "queue": pickle.dumps(self._pqueues[partition_number].queue),
                    }
                    message = pickle.dumps(message)
                    await self.send_message_to_broker(host, port, message)

    async def handle_client(self, reader, writer):
        while True:
            data = await reader.read(1024)
            addr = writer.get_extra_info("peername")
            try:
                message = data.decode()
            except:
                message = pickle.loads(data)
                await self.handle_broker_message(reader, writer, addr, message)
                break

            print(f"Received {message} from {addr}")
            json_dict = self.extract_message(message)
            if json_dict["type"] == "PUSH":
                status = self._push(json_dict)
                print(f"Status: {status}")
                response = (
                    str(SOCKET_STATUS.WRITE_SUCCESS.value)
                    if status == STATUS.SUCCESS
                    else str(SOCKET_STATUS.WRITE_FAILED.value)
                )
                await self.update_replicas()
                writer.write((response + "\n").encode())
                await writer.drain()
            elif json_dict["type"] == "PULL":
                message = self._pull(json_dict)
                message = str(message)
                response = message + "\n" if message is not None else "No message\n"
                await self.update_replicas()
                writer.write(response.encode())
                await writer.drain()
            elif json_dict["type"] == "SUBSCRIBE":
                status = self._subscribe(addr, None, writer, json_dict["part_no"])
                response = (
                    str(SOCKET_STATUS.SUBSCRIBE_SUCCESS.value)
                    if status == STATUS.SUCCESS
                    else str(SOCKET_STATUS.SUBSCRIBE_FAILED.value)
                )
                print(f"Response: {response}")
                writer.write((response + "\n").encode())
            else:
                break

        self._logger.info(f"Closing the connection")
        writer.close()
        await writer.wait_closed()

    async def read_root(self):
        response_200_metrics.inc()
        return fastapi.Response(content="Hello, World", status_code=200)

    async def get_zookeeper(self):
        response_200_metrics.inc()
        return fastapi.Response(content=json.dumps(self._zookeeper), status_code=200)

    async def gen_metrics(self):
        self._logger.info(f"inside gen_metrics")
        response_200_metrics.inc()
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        data = generate_latest(registry)
        return fastapi.Response(
            content=data, media_type=CONTENT_TYPE_LATEST, status_code=200
        )

    async def socket_thread(self):
        server = await asyncio.start_server(
            self.handle_client, self._host, self._socket_port
        )

        addr = server.sockets[0].getsockname()
        print(f"Serving on {addr}")

        async with server:
            await server.serve_forever()

    def initiate(self):
        # Create socket
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                client_socket.connect(
                    (self._zookeeper["host"], self._zookeeper["socket_port"])
                )
                broker_info = f"initiate :{self._host}:{self._socket_port}:{self.ping_port}:{self.id}"
                client_socket.sendall(broker_info.encode())
                print("Broker initiated and connected to the leader.")
        except:
            print("Broker could not connect to the leader.")

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

        self.initiate()
        http_thread = threading.Thread(
            target=asyncio.run, args=(uvicorn.run(app, host=host, port=int(http_port)),)
        )
        http_thread.start()

    # metrics section
    def is_zookeeper(self):
        return self.is_zookeeper_nominee


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

    broker = Broker(args.host, args.socket_port, args.http_port, args.ping_port)
    broker.run(args.host, args.http_port, args.socket_port)
