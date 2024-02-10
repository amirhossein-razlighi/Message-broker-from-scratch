import unittest
import requests
from time import sleep
import sys
import threading

sys.path.append("../../")
sys.path.append("../")

from server.broker import Broker

class TestAPI(unittest.TestCase):
    def test_api_sanity(self):
        broker = Broker("127.0.0.1", 8888, 8000, 7500)
        broker_thread = threading.Thread(target=broker.run, daemon=True)
        broker_thread.start()
        sleep(1)
        host = "127.0.0.1"
        port = 8000
        response = requests.get(f"http://{host}:{port}/zookeeper")
        self.assertEqual(response.status_code, 200)
