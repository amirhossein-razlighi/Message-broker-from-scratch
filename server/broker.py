import queue
import asyncio
import socket
import json
import fastapi
import argparse

app = None

class Broker:
    def __init__(self, host, port):
        self._queue = queue.Queue()
        self._replica_queue = queue.Queue()
        self._app = app
        self._host = host
        self._port = port

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
        return {"Hello": "World"}
    
    async def get_zookeeper(self):
        return {"zookeeper": "zookeeper"}

    async def main(self):
        server = await asyncio.start_server(self.handle_client, self._host, self._port)

        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')

        async with server:
            await server.serve_forever()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", default=8888, help="Port to bind to")
    args = parser.parse_args()
    print(args.host, args.port)
    app = fastapi.FastAPI(port=args.port, host=args.host)

    broker = Broker(args.host, args.port)

    app.add_api_route("/", broker.read_root, methods=["GET"])
    app.add_api_route("/zookeeper", broker.get_zookeeper, methods=["GET"])

    asyncio.run(broker.main())