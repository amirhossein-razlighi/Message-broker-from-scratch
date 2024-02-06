import queue
import asyncio
import socket
import json

class Broker:
    def __init__(self):
        self._queue = queue.Queue()
        self._replica_queue = queue.Queue()

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


    async def main(self):
        server = await asyncio.start_server(
            self.handle_client, '127.0.0.1', 8888)

        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')

        async with server:
            await server.serve_forever()

if __name__ == '__main__':
  broker = Broker()
  asyncio.run(broker.main())