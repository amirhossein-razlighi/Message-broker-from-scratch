import unittest
import requests
from time import sleep
import sys
import threading

sys.path.append("../")

from broker import Broker

class TestAPI(unittest.TestCase):
    def test_api_sanity(self):
        broker = Broker("127.0.0.1", 8001, 8002, 8003)
        broker_thread = threading.Thread(target=broker.run, daemon=True)
        broker_thread.start()
        sleep(1)
        host = "127.0.0.1"
        port = 8001
        response = requests.get(f"http://{host}:{port}/zookeeper")
        self.assertEqual(response.status_code, 200)
