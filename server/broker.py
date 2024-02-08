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

app = None

class Broker:
    def __init__(self, host, port):
        self.id = str(uuid.uuid4())
        self._pqueues = {}
        # self._queue = queue.Queue()
        # self._replica_queue = queue.Queue()
        self._app = app
        self._host = host
        self._port = port
        self._zookeeper = {"host": None, "http_port": None, "socket_port": None}
        self._create_pqueue(0, False)
        logging.basicConfig(
            level=logging.DEBUG, filename=f"logs/broker_{self.id}.log", filemode="w"
            ,format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
        self._logger = logging.getLogger(__name__)

    def _create_pqueue(self, part_no, is_replica):
        self._pqueues[part_no] = Pqueue(part_no, is_replica)

    def _read(self, part_no):
        return self._pqueues[part_no].read()

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

    async def handle_client(self, reader, writer):
        data = await reader.read(100)
        message = data.decode()
        addr = writer.get_extra_info('peername')
        print(f"Received {message} from {addr}")

        json_dict = self.extract_message(message)
        if json_dict["type"] == "PUSH":
            self._logger.info(f"Received PUSH message {json_dict}")
            part_no = int(json_dict["part_no"])
            if part_no not in self._pqueues:
                # error no such queue
                self._logger.error(f"Invalid part_no {part_no}")
                return "Invalid"
            self._logger.info(f"Writing message {json_dict['value']} to part_no {part_no}")
            message = self._pqueues[part_no].write_new_message(json_dict["value"])
            self._logger.info(f"Message written to part_no {part_no}")
            writer.write(b"Message written")
            await writer.drain()
        elif json_dict["type"] == "PULL":
            part_no = json_dict["part_no"]
            if part_no not in self._pqueues:
                # error no such queue
                return "Invalid"
            message = self._read(part_no)
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
            self.handle_client, self._host, self._port)

        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')

        async with server:
            await server.serve_forever()
    
    def run(self, host, http_port, socket_port):
        app = fastapi.FastAPI(port=http_port, host=host)

        app.add_api_route("/", self.read_root, methods=["GET"])
        app.add_api_route("/zookeeper", self.get_zookeeper, methods=["GET"])

        socket_thread = threading.Thread(target=asyncio.run, args=(self.socket_thread(),))
        socket_thread.start()
        http_thread = threading.Thread(target=asyncio.run, args=(uvicorn.run(app, host=host, port=http_port),))
        http_thread.start()



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--socket_port", default=8000, help="Port to bind socket connection to")
    parser.add_argument("--http_port", default=8888, help="Port to bind https connection to")
    args = parser.parse_args()
    print(args.host, args.socket_port, args.http_port)

    broker = Broker(args.host, args.socket_port)
    broker.run(args.host, args.http_port, args.socket_port)
