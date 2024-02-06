import asyncio
import socket
import json
from pqueue import Pqueue


class Broker:

    def __init__(self, id):
        self.id = id
        self._pqueues = {}

    def _create_pqueue(self, part_no, is_replica):
        self._pqueues[part_no] = Pqueue(part_no, is_replica)

    def _read(self, part_no):
        return self._pqueues[part_no].read()

    def extract_message(self, message):
        statements = message.split(",")
        json_dict = {}
        for statement in statements:
            key, value = statement.split(":")
            key = key.split('"')[1]
            value = value.split('"')[1]
            json_dict[key] = value
        return json_dict

    async def handle_client(self, reader, writer):
        data = await reader.read(100)
        message = data.decode()
        addr = writer.get_extra_info('peername')
        print(f"Received {message} from {addr}")

        json_dict = self.extract_message(message)
        if json_dict["type"] == "PUSH":
            part_no = json_dict["part_no"]
            if part_no not in self._pqueues:
                # error no such queue
                return "Invalid"
            message = self._pqueues[part_no].write_new_message(json_dict["value"])
            writer.write(message.encode())
        elif json_dict["type"] == "PULL":
            part_no = json_dict["part_no"]
            if part_no not in self._pqueues:
                # error no such queue
                return "Invalid"
            message = self._read(part_no)
            writer.write(message.encode())
        else:
            writer.write("Invalid".encode())
        # await writer.drain()
        # print("Close the connection")
        # writer.close()

    async def main(self):
        server = await asyncio.start_server(
            self.handle_client, '127.0.0.1', 8888)

        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')

        async with server:
            await server.serve_forever()


if __name__ == '__main__':
    broker = Broker(1)
    asyncio.run(broker.main())
