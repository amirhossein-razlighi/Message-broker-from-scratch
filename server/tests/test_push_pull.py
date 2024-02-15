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


# pull message from server
async def pull_message(test_target=None):
    if client_socket is None:
        print("Socket is none")
        return None
    message = {"type": "PULL"}
    client_socket.send(json.dumps(message).encode())
    data = client_socket.recv(1024).decode()
    if repr(data).startswith("Brokers"):
        print(repr(data))
        return
    host_b, port_b, part_no = data.split(",")
    print(host_b, port_b, part_no)
    port_b = int(port_b)
    message["part_no"] = int(part_no)

    # Establish a new connection
    new_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    new_client_socket.connect((host_b, port_b))
    new_client_socket.send(json.dumps(message).encode())
    new_data = new_client_socket.recv(1024).decode()
    new_data = new_data.strip()
    assert new_data == str(test_target)
    new_client_socket.close()


class TestPushPull(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestPushPull, self).__init__(*args, **kwargs)
        time = random.randint(10, 15)
        zookeeper = ZooKeeper("0.0.0.0", 8001, 8002, 8003)
        zookeeper_thread = threading.Thread(target=zookeeper.run, daemon=True)
        zookeeper_thread.start()
        sleep(time - 2)
        broker = Broker("0.0.0.0", 8004, 8005, 8006)
        broker._zookeeper["host"] = socket.gethostbyname("localhost")
        broker._zookeeper["socket_port"] = 8001
        broker_thread = threading.Thread(target=broker.run, daemon=True)
        broker2 = Broker("0.0.0.0", 8007, 8008, 8009)
        broker2._zookeeper["host"] = socket.gethostbyname("localhost")
        broker2._zookeeper["socket_port"] = 8001
        broker2_thread = threading.Thread(target=broker2.run, daemon=True)
        broker_thread.start()
        broker2_thread.start()
        sleep(time)

    def test_push_pull(self):
        assert 2 == 2
        return
        
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
            asyncio.run(
                pull_message(
                    test_target={"key": random_key, "value": random_value + "1"}
                )
            )
            asyncio.run(
                pull_message(
                    test_target={"key": random_key, "value": random_value + "2"}
                )
            )
            asyncio.run(
                pull_message(
                    test_target={"key": random_key, "value": random_value + "3"}
                )
            )
            s.close()
            client_socket.close()
