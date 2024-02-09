import asyncio
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

app = None


class Broker:
    def __init__(self, host, socket_port, http_port):
        self.id = str(uuid.uuid4())
        self._pqueues = {}
        # self._queue = queue.Queue()
        # self._replica_queue = queue.Queue()
        self._app = app
        self._host = host
        self._socket_port = socket_port
        self._http_port = http_port
        self._zookeeper = {
            "host": host,
            "http_port": http_port,
            "socket_port": socket_port,
        }
        self._create_pqueue(0, False)
        self._broker_subscribers = []
        logging.basicConfig(
            level=logging.DEBUG,
            filename=f"logs/broker_{self.id}.log",
            filemode="w",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        self.is_up = 0
        self.is_empty = 1
        self._logger = logging.getLogger(__name__)

    def _create_pqueue(self, part_no, is_replica):
        self._pqueues[part_no] = Pqueue(part_no, is_replica)

    def _read(self, part_no):
        message = self._pqueues[part_no].read()
        self._logger.info(f"Read message {message} from part_no {part_no}")
        return message

    def extract_message(self, message):
        statements = message.split(",")
        json_dict = {}
        print(statements)
        for statement in statements:
            key, value = statement.split(":")
            key = key.split('"')[1]
            value = value.split('"')[1]
            print(key, value)
            json_dict[key] = value
        return json_dict

    def _push(self, json_dict):
        self._logger.info(f"Received PUSH message {json_dict}")
        part_no = int(json_dict["part_no"])
        if part_no not in self._pqueues:
            self._logger.error(f"Invalid part_no {part_no}")
            return STATUS.ERROR
        self._logger.info(f"Writing message {json_dict['value']} to part_no {part_no}")
        message = self._pqueues[part_no].write_new_message(json_dict["value"])
        self._logger.info(f"Message written to part_no {part_no}")

        if self._broker_subscribers:
            selected_subscriber = random.choice(self._broker_subscribers)
            self._logger.info(f"Selected subscriber {selected_subscriber}")
            self._logger.info(f"Sending message to subscriber {selected_subscriber}")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.connect((selected_subscriber.host, selected_subscriber.socket_port))
                    s.sendall(
                        f"MESSAGE:{message}".encode()
                    )
                    data = s.recv(1024)
                    self._logger.info(f"Received {data} from subscriber {selected_subscriber}")
                except Exception as e:
                    self._logger.error(f"Error while sending message to subscriber {selected_subscriber}")
                    self._logger.error(e)
                    #TODO: Implement unsubscription
                    return STATUS.ERROR
        return STATUS.SUCCESS

    def _pull(self, json_dict):
        self._logger.info(f"Received PULL message {json_dict}")
        part_no = int(json_dict["part_no"])
        if part_no not in self._pqueues:
            self._logger.error(f"Invalid part_no {part_no}")
            part_no = random.choice(list(self._pqueues.keys()))
            self._logger.info(f"Selected part_no {part_no}")
        self._logger.info(f"Reading message from part_no {part_no}")
        return self._read(part_no)
        
    def _subscribe(self, subscriber, broker_id):
        self._logger.info(f"Received SUBSCRIBE message from {subscriber}")
        self._logger.info(f"Subscriber {subscriber} subscribed to broker {broker_id}")
        self._broker_subscribers.append(subscriber)
        return STATUS.SUCCESS

    async def handle_client(self, reader, writer):
        data = await reader.read(100)
        message = data.decode()
        addr = writer.get_extra_info("peername")
        print(f"Received {message} from {addr}")

        json_dict = self.extract_message(message)
        if json_dict["type"] == "PUSH":
            status = self._push(json_dict)
            writer.write(
                SOCKET_STATUS.WRITE_SUCCESS.value.encode()
                if status == STATUS.SUCCESS
                else SOCKET_STATUS.WRITE_FAILURE.value.encode()
            )
            await writer.drain()
        elif json_dict["type"] == "PULL":
            message = self._pull(json_dict)
            writer.write(message.encode())
            await writer.drain()
        elif json_dict["type"] == "SUBSCRIBE":
            status = self._subscribe(json_dict["subscriber"], json_dict["broker_id"])
            if status == STATUS.SUCCESS:
                writer.write(SOCKET_STATUS.WRITE_SUCCESS.value.encode())
            else:
                writer.write(SOCKET_STATUS.WRITE_FAILURE.value.encode())
        else:
            writer.write("Invalid".encode())

        self._logger.info(f"Closing the connection")
        writer.close()
        # await writer.drain()
        # print("Close the connection")
        # writer.close()

    async def read_root(self):
        return fastapi.Response(content="Hello, World", status_code=200)

    async def get_zookeeper(self):
        return fastapi.Response(content=json.dumps(self._zookeeper), status_code=200)

    async def socket_thread(self):
        server = await asyncio.start_server(
            self.handle_client, self._host, self._socket_port
        )

        addr = server.sockets[0].getsockname()
        print(f"Serving on {addr}")

        async with server:
            await server.serve_forever()

    def run(self, host, http_port, socket_port):
        app = fastapi.FastAPI(port=http_port, host=host)

        app.add_api_route("/", self.read_root, methods=["GET"])
        app.add_api_route("/zookeeper", self.get_zookeeper, methods=["GET"])

        socket_thread = threading.Thread(
            target=asyncio.run, args=(self.socket_thread(),)
        )
        socket_thread.start()
        http_thread = threading.Thread(
            target=asyncio.run, args=(uvicorn.run(app, host=host, port=int(http_port)),)
        )
        http_thread.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument(
        "--socket_port", default=8000, help="Port to bind socket connection to"
    )
    parser.add_argument(
        "--http_port", default=8888, help="Port to bind https connection to"
    )
    args = parser.parse_args()
    print(args.host, args.socket_port, args.http_port)

    broker = Broker(args.host, args.socket_port, args.http_port)
    broker.run(args.host, args.http_port, args.socket_port)
