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

class TestPush(unittest.TestCase):
    def test_push(self):
        zookeeper = ZooKeeper("127.0.0.1", 8001, 8002, 8003)
        zookeeper_thread = threading.Thread(target=zookeeper.run, daemon=True)
        zookeeper_thread.start()
        sleep(1)
        broker = Broker("127.0.0.1", 8000, 8888, 7500)
        broker._zookeeper["host"] = "127.0.0.1"
        broker._zookeeper["socket_port"] = 8001
        broker_thread = threading.Thread(target=broker.run, daemon=True)
        broker_thread.start()
        sleep(1)

        host = "127.0.0.1"
        port = 8000
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            random_key = "wow" + str(random.randint(0, 1000))
            random_value = "bye" + str(random.randint(0, 1000))
            message = {"type": "PUSH", "key": random_key, "value": random_value, "part_no": "0"}
            s.sendall(json.dumps(message).encode())
            data = s.recv(1024).decode()
            data = data.strip()
            assert data == str(status.SOCKET_STATUS.WRITE_SUCCESS.value)
            s.close()