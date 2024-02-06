import unittest
import requests
import asyncio

class TestAPI(unittest.TestCase):
    def test_api(self):
        host = "127.0.0.1"
        port = 8000
        response = requests.get(f"http://{host}:{port}/zookeeper")
        self.assertEqual(response.status_code, 200)


async def run_tests():
    print("Running tests...")
    response = requests.get("http://127.0.0.1:8888/")
    print(response.text)

if __name__ == "__main__":
    asyncio.run(run_tests())