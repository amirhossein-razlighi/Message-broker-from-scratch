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

from metrics import *

app = None

uvicorn.logging.logging.basicConfig(level=uvicorn.logging.logging.DEBUG)


class Broker:
    def __init__(self, host, socket_port, http_port, ping_port):
        self.id = str(uuid.uuid4())
        self._pqueues = {}
        # self._queue = queue.Queue()
        # self._replica_queue = queue.Queue()
        self._app = app
        self._host = host
        self._socket_port = int(socket_port)
        self._http_port = http_port
        self._zookeeper = {
            "host": os.getenv('ZOOKEEPER'),
            "http_port": 8888,
            "socket_port": 8000,
            "ping_port": 7500,
        }
        self._create_pqueue(0, False)
        self._broker_subscribers = []
        self._logger = logging.getLogger(__name__)
        self._observers = set()
        self._is_replica = False
        self.ping_port = ping_port
        self.is_zookeeper_nominee = False
    
    def __str__(self):
        return f"Broker(id={self.id}, pqueues={self._pqueues}, is_replica={self._is_replica}, observers={self._observers})"

    def register(self, observer):
        self._observers.add(observer)

    def unregister(self, observer):
        self._observers.remove(observer)


    def _create_pqueue(self, part_no, is_replica):
        self._pqueues[part_no] = Pqueue(part_no, is_replica)

    def _read(self, part_no):
        message = self._pqueues[part_no].read()
        if message is not None:
            self._logger.info(f"Read message {message} from part_no {part_no}")
        else:
            message = "No message"
        return message

    def extract_message(self, message):
        try:
            statements = message.split(",")
            json_dict = {}
            print(statements)
            for statement in statements:
                key, value = statement.split(":")
                key = key.split('"')[1]
                value = value.split('"')[1]
                # print(key, value)
                json_dict[key] = value
        except:
            json_dict = {"type": "Invalid"}
        return json_dict

    async def _subscribe_checker_and_notifier(self, writer):
        while True:
            try:
                if not len(self._broker_subscribers) == 0 and (
                    (message := self._read(0)) is not None
                ):
                    if message == "No message":
                        await asyncio.sleep(5)
                        continue
                    selected_subscriber = random.choice(self._broker_subscribers)
                    self._logger.info(f"Selected subscriber {selected_subscriber}")
                    self._logger.info(
                        f"Sending message {message} to subscriber {selected_subscriber}"
                    )
                    print(f"Sending message to subscriber {selected_subscriber}")
                    response = message + "\n"
                    await asyncio.wait_for(writer.write(response.encode()), timeout=5)
                    # await writer.drain()
                    print(f"Message sent to subscriber {selected_subscriber}")
                    self._logger.info(
                        f"Message sent to subscriber {selected_subscriber}"
                    )
            except:
                continue
            await asyncio.sleep(5)

    def _push(self, json_dict):
        self._logger.info(f"Received PUSH message {json_dict}")
        print(f"Received PUSH message {json_dict}")
        part_no = int(json_dict["part_no"])
        if part_no not in self._pqueues:
            self._logger.error(f"Invalid part_no {part_no}")
            print(f"Invalid part_no {part_no}")
            return STATUS.ERROR
        self._logger.info(f"Writing message {json_dict['value']} to part_no {part_no}")
        print(f"Writing message {json_dict['value']} to part_no {part_no}")
        message = self._pqueues[part_no].write_new_message(json_dict["value"])
        self._logger.info(f"Message written to part_no {part_no}")
        print(f"Message written to part_no {part_no}")
        self.is_empty = 0  # TODO ?
        # write notify for each replica
        print(f"Broker {self.id} notifying observers...")
        for observer in self._observers:
            if observer.is_replica:
                self._logger.info(f"Writing message {json_dict['value']} to replica part_no {part_no}")
                print(f"Received PUSH message for Replica {json_dict}")
                print(f"Writing message {json_dict['value']} to replica part_no {observer.part_no}")
                message = self._pqueues[observer.part_no].write_new_message(json_dict["value"])
                self._logger.info(f"Message written to part_no {observer.part_no}")
                print(f"Message written to part_no {observer.part_no}")

        return STATUS.SUCCESS

    def _pull(self, json_dict):
        self._logger.info(f"Received PULL message {json_dict}")
        pull_request_metrics.inc()
        try:
            part_no = int(json_dict["part_no"])
            self._pqueues[part_no]
        except:

            part_no = random.choice(list(self._pqueues.keys()))
            self._logger.info(f"Selected part_no {part_no}")

        self._logger.info(f"Reading message from part_no {part_no}")
        return self._read(part_no)

    def _subscribe(self, subscriber, broker_id, writer):
        subscriber = {"host": subscriber[0], "socket_port": subscriber[1]}
        self._logger.info(f"Received SUBSCRIBE message from {subscriber}")
        self._logger.info(f"Subscriber {subscriber} subscribed to broker {broker_id}")
        self._broker_subscribers.append(subscriber)
        asyncio.create_task(self._subscribe_checker_and_notifier(writer))
        return STATUS.SUCCESS

    async def handle_client(self, reader, writer):
        while True:
            data = await reader.read(100)
            message = data.decode()
            addr = writer.get_extra_info("peername")
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
                writer.write((response + "\n").encode())
                await writer.drain()
            elif json_dict["type"] == "PULL":
                message = self._pull(json_dict)
                response = message + "\n" if message is not None else "No message\n"
                writer.write(response.encode())
                await writer.drain()
            elif json_dict["type"] == "SUBSCRIBE":
                status = self._subscribe(addr, json_dict["broker_id"], writer)
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
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        data = generate_latest(registry)
        return fastapi.Response(content=data, media_type=CONTENT_TYPE_LATEST, status_code=200)

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
                client_socket.connect((self._zookeeper['host'], self._zookeeper['socket_port']))
                broker_info = f"initiate :{self._host}:{self._socket_port}:{self.ping_port}:{self.id}"
                client_socket.sendall(broker_info.encode())
                print("Broker initiated and connected to the leader.")
        except:
            print("Broker could not connect to the leader.")

    def run(self, host=None, http_port=None, socket_port=None):
        self.initiate()
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
