import queue
import asyncio
import socket
import json
import fastapi
import argparse
import uvicorn
import threading
import uuid

app = None

class Broker:
    def __init__(self, host, port):
        self.id = str(uuid.uuid4())
        self._queue = queue.Queue()
        self._replica_queue = queue.Queue()
        self._app = app
        self._host = host
        self._port = port
        self._zookeeper = {"host": None, "http_port": None, "socket_port": None}

    def publish(self, data):
        self._queue.put(data)

    def consume(self):
        return self._queue.get()

    def extract_message(self, message):
      statements = message.split(",")
      json_dict = {}
      for statement in statements:
          key, value = statement.split(":")
          key = key.split('"')[1]
          value = value.split('"')[1]
          json_dict[key] = value
      return json_dict

    def print_queue(self):
        print(self._queue.queue)
    
    async def push(self, json_dict, writer):
        self.publish(json_dict)
        print("Published message")
        response = {"status": "OK", "message": "Message received"}
        writer.write(json.dumps(response).encode())
        await writer.drain()

    async def handle_client(self, reader, writer):
        data = await reader.read(100)
        message = data.decode()
        message = message[1:-2]
        addr = writer.get_extra_info('peername')
        print(f"Received {message} from {addr} with type {type(message)}")
        json_dict = self.extract_message(message)
        print(json_dict)
        print(json_dict.keys())
        print(json_dict.values())

        if json_dict["type"] == "PUSH":
            await self.push(json_dict, writer)
            self.print_queue()

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