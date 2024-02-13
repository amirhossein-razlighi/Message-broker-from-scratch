import unittest
import random
import socket
import json
import sys
import threading
from time import sleep
import asyncio

sys.path.append("../")
sys.path.append("../../")

import status
from broker import Broker
from zookeeper import ZooKeeper

client_socket = None

async def push_message(key: str, value: str):
    if client_socket is None:
        # TODO return error
        return None
    message = {"type": "PUSH", "key": key, "value": value}

    client_socket.send(json.dumps(message).encode())
    data = client_socket.recv(1024).decode()
    assert data == str(status.SOCKET_STATUS.WRITE_SUCCESS.value)

class TestPush(unittest.TestCase):
    def test_push(self):
        zookeeper = ZooKeeper("127.0.0.1", 8001, 8002, 8003)
        zookeeper_thread = threading.Thread(target=zookeeper.run, daemon=True)
        zookeeper_thread.start()
        sleep(1)
        broker = Broker("127.0.0.1", 8004, 8005, 8006)
        broker._zookeeper["host"] = socket.gethostbyname("localhost")
        broker._zookeeper["socket_port"] = 8001
        broker_thread = threading.Thread(target=broker.run, daemon=True)
        broker_thread.start()
        sleep(1)
        broker = Broker("127.0.0.1", 8007, 8008, 8009)
        broker._zookeeper["host"] = socket.gethostbyname("localhost")
        broker._zookeeper["socket_port"] = 8001
        broker_thread = threading.Thread(target=broker.run, daemon=True)
        broker_thread.start()
        sleep(1)

        host = "localhost"
        port = 8001
        global client_socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            client_socket = s
            s.connect((host, port))
            random_key = "wow" + str(random.randint(0, 1000))
            random_value = "bye" + str(random.randint(0, 1000))
            asyncio.run(push_message(random_key, random_value + "1"))
            asyncio.run(push_message(random_key, random_value + "2"))
            asyncio.run(push_message(random_key, random_value + "3"))
            s.close()