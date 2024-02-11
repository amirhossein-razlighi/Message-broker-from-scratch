import unittest
import requests
from time import sleep
import sys
import threading

sys.path.append("../")
sys.path.append("../../")

from broker import Broker
from zookeeper import ZooKeeper

class TestAPI(unittest.TestCase):
    def test_api_sanity(self):
        zookeeper = ZooKeeper("127.0.0.1", 8001, 8002, 8003)
        zookeeper_thread = threading.Thread(target=zookeeper.run, daemon=True)
        zookeeper_thread.start()
        sleep(1)
        broker = Broker("127.0.0.1", 8004, 8005, 8006)
        broker._zookeeper["host"] = "127.0.0.1"
        broker._zookeeper["socket_port"] = 8001
        broker_thread = threading.Thread(target=broker.run, daemon=True)
        broker_thread.start()
        sleep(1)
        host = "127.0.0.1"
        port = 8005
        response = requests.get(f"http://{host}:{port}/zookeeper")
        self.assertEqual(response.status_code, 200)