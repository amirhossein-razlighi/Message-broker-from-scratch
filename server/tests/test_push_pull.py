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


class TestPushPull(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestPushPull, self).__init__(*args, **kwargs)
        self.zookeeper = ZooKeeper("127.0.0.1", 8001, 8002, 8003)
        self.zookeeper_thread = threading.Thread(target=self.zookeeper.run, daemon=True)
        self.zookeeper_thread.start()
        sleep(3)
        self.broker = Broker("127.0.0.1", 8004, 8005, 8006)
        self.broker._zookeeper["host"] = socket.gethostbyname("localhost")
        self.broker._zookeeper["socket_port"] = 8001
        self.broker_thread = threading.Thread(target=self.broker.run, daemon=True)
        self.broker_thread.start()
        sleep(3)
        self.broker = Broker("127.0.0.1", 8007, 8008, 8009)
        self.broker._zookeeper["host"] = socket.gethostbyname("localhost")
        self.broker._zookeeper["socket_port"] = 8001
        self.broker_thread = threading.Thread(target=self.broker.run, daemon=True)
        self.broker_thread.start()

    def test_push_pull(self):
        host = socket.gethostbyname("localhost")
        port = 8001
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            for _ in range(10):
                random_key = "wow" + str(random.randint(0, 1000))
                random_value = "bye" + str(random.randint(0, 1000))
                message = {
                    "type": "PUSH",
                    "key": random_key,
                    "value": random_value,
                }
                s.sendall(json.dumps(message).encode())
                data = s.recv(1024).decode()
                data = data.strip()
                assert data == str(status.SOCKET_STATUS.WRITE_SUCCESS.value)

                message = {
                    "type": "PULL",
                }
                s.sendall(json.dumps(message).encode())
                data = s.recv(1024).decode()
                data = data.strip()
                host_b, port_b = data.split(',')
                port_b = int(port_b)

                # Establish a new connection
                new_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                new_client_socket.connect((host_b, port_b))
                new_client_socket.send(json.dumps(message).encode())
                new_data = new_client_socket.recv(1024).decode()
                
                new_client_socket.close()
                assert new_data.strip() == str({"key": random_key, "value": random_value})
            s.close()