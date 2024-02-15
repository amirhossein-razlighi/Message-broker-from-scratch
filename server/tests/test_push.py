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
        return None
    message = {"type": "PUSH", "key": key, "value": value}

    client_socket.send(json.dumps(message).encode())
    data = client_socket.recv(1024).decode()
    assert data == str(status.SOCKET_STATUS.WRITE_SUCCESS.value)


class TestPush(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestPush, self).__init__(*args, **kwargs)
    
    def test_push(self):
        time = random.randint(10, 15)
        zookeeper = ZooKeeper("0.0.0.0", 8001, 8002, 8003)
        zookeeper_thread = threading.Thread(target=zookeeper.run, daemon=True)
        zookeeper_thread.start()
        sleep(time - 2)
        broker2 = Broker("0.0.0.0", 8004, 8005, 8006)
        broker2._zookeeper["host"] = socket.gethostbyname("localhost")
        broker2._zookeeper["socket_port"] = 8001
        broker2_thread = threading.Thread(target=broker2.run, daemon=True)
        broker = Broker("0.0.0.0", 8007, 8008, 8009)
        broker._zookeeper["host"] = socket.gethostbyname("localhost")
        broker._zookeeper["socket_port"] = 8001
        broker_thread = threading.Thread(target=broker.run, daemon=True)

        broker2_thread.start()
        broker_thread.start()
        sleep(time)

        host = "0.0.0.0"
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
