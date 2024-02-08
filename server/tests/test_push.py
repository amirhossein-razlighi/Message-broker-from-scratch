import unittest
from server.broker import Broker
import random
import socket
import json


class TestPush(unittest.TestCase):
    def test_push(self):
        host = "127.0.0.1"
        # port = random.randint(7500, 8500)
        port = 8000
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            random_key = "wow" + str(random.randint(0, 1000))
            random_value = "bye" + str(random.randint(0, 1000))
            message = {"key": random_key, "value": random_value, "type": "PUSH"}
            s.sendall(json.dumps(message).encode())
            data = s.recv(1024)
            data = repr(data)
            assert data == 'b\'{"status": "OK", "message": "Message received"}\''


if __name__ == '__main__':
    unittest.main()
