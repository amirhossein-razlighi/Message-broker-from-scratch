import threading

import requests
import json
import socket
from time import sleep
from typing import Callable
import os
import argparse
import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
import random
import sys

sys.path.append("../")
executor = ThreadPoolExecutor(max_workers=1)

# from server.broker import Broker
# from server.replica import Replica
# from server.zookeeper import ZooKeeper

zookeeper_ips = ['127.0.0.1']
port = 8000
master_ip = None
client_socket = None
client_subscribe_socket = None

logging.log(logging.INFO, "Client started")

# find the master zookeeper
def find_master():
    for ip in zookeeper_ips:
        response = requests.get(f"{ip}/zookeeper")
        if response.status_code == 200:
            return ip
    return None

# open connection with server
def open_connection(node_ip, node_port):
    host_ip = socket.gethostbyname(node_ip)
    port_cnn = node_port
    new_socket = socket.socket()
    new_socket.connect((host_ip, port_cnn))
    print("Connected to server")
    return new_socket

async def get_input(prompt):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, input, prompt)

# push message to server
async def push_message(key: str, value: str):
    if client_socket is None:
        # TODO return error
        return None
    message = {"type": "PUSH", "key": key, "value": value, "part_no": "0"}

    client_socket.send(json.dumps(message).encode())
    data = client_socket.recv(1024).decode()
    print(f"Received from server: {repr(data)}")

# pull message from server
async def pull_message():
    if client_socket is None:
        # TODO return error
        return None
    message = {
        "type": "PULL"
    }
    client_socket.send(json.dumps(message).encode())
    data = client_socket.recv(1024).decode()
    if repr(data).startswith('Brokers'):
        print(repr(data))
        return
    host_b, port_b = data.split(',')
    port_b = int(port_b)

    # Establish a new connection
    new_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    new_client_socket.connect((host_b, port_b))
    new_client_socket.send(json.dumps(message).encode())
    new_data = new_client_socket.recv(1024).decode()
    new_data = new_data.strip()
    print(f"Received from server: {repr(new_data)}")
    new_client_socket.close()


async def subscribe(f: Callable):
    global client_subscribe_socket
    if client_socket is None:
        # TODO return error
        return None

    message_0 = {
        "type": "SUBSCRIBE",
        # "broker_id": "0"
    }

    client_socket.send(json.dumps(message_0).encode())
    data = client_socket.recv(1024).decode()
    if repr(data).startswith("There is not"):
        print(repr(data))
        return
    host_b, port_b, bid = data.split(',')
    port_b = int(port_b)
    client_subscribe_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_subscribe_socket.connect((host_b, port_b))
    message_1 = {"type": "SUBSCRIBE",
                 "broker_id": bid
                 }
    client_subscribe_socket.send(json.dumps(message_1).encode())
    new_data = client_subscribe_socket.recv(1024).decode()

    print(f"Received from server: {repr(new_data)}")
    threading.Thread(target=receive_message, daemon=True, args=(f,)).start()


# a function to receive messages from the server (if we are subscribed)
# but if response doesn't come in 5 seconds, we continue with our work
def receive_message(f:Callable):
    global client_subscribe_socket
    client_socket.settimeout(3)
    while True:
        try:
            data = client_subscribe_socket.recv(1024).decode()
            if not repr(data).startswith('No message'):
                data = f(repr(data).strip)
            print(f"Received from server: {repr(data)}")
        except:
            continue


async def main():
    global client_socket
    global master_ip

    # master_ip = find_master()
    # if master_ip is None:
    #     print("No master found")
    #     return

    host_name = os.getenv("ZOOKEEPER")
    client_socket = open_connection(host_name, port)

    rand_key = random.randint(0, 100)
    rand_value = random.randint(0, 100)
    await push_message(f"{rand_key}", f"world {rand_value}")
    await push_message(f"{rand_key}", f"world {rand_value + 1}")
    await push_message(f"{rand_key}", f"world {rand_value + 2}")
    await pull_message()
    await pull_message()
    await pull_message()

    """ TEST SUBSCRIBE/PUSH/PULL
    loop = asyncio.get_event_loop()
    loop.run_until_complete(subscribe(None))

    sleep(6)

    for i in range(10):
        push_message(f"{i}", f"world {i}")
        push_message(f"{i}", f"world {i + 1}")
        push_message(f"{i}", f"world {i + 2}")
    
    while True:
        print("Pushing messages")
        for _ in range(10):
            key = "key " + str(random.randint(0, 100))
            value = "value " + str(random.randint(0, 100))
            await push_message(key, value)
        print("Pulling messages")
        await pull_message()
        break
    """

    """ Interactive TEST:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        client_socket = s
        s.connect((host_name, port))
        while True:
            print("1. Push message")
            print("2. Pull message")
            print("3. Subscribe")
            print("4. Exit")
            choice = await get_input("Enter choice: ")
            choice = int(choice)
            if choice == 1:
                key = await get_input("Enter key: ")
                value = await get_input("Enter value: ")
                await push_message(key, value)
            elif choice == 2:
                await pull_message()
            elif choice == 3:
                await subscribe(None)
            elif choice == 4:
                break
            else:
                print("Invalid choice")
        client_socket.close()
    """

# Example test scenarios in the client

def test_add_brokers_and_replicas():
    # Initialize ZooKeeper (you can adapt this based on your actual setup)
    zookeeper = ZooKeeper("127.0.0.1", 8000, 8888, 7500)
    zookeeper_thread = threading.Thread(target=zookeeper.run, daemon=True)
    zookeeper_thread.start()
    sleep(1)
    broker1 = Broker("127.0.0.1", 8000, 8888, 7500)
    broker_thread = threading.Thread(target=broker1.run, daemon=True)
    broker_thread.start()
    sleep(1)



def test_heartbeat_and_health_check():
    # Initialize ZooKeeper (you can adapt this based on your actual setup)
    host_name = os.getenv("BROKER")
    zookeeper = ZooKeeper(host_name, 8000, 8888, 7500)

    # Simulate sending heartbeats from brokers

    broker1 = Broker(host_name, 9000, 9500, 1)
    broker2 = Broker(host_name, 9001, 9501, 2)
    zookeeper.add_broker(broker1, partition=0)
    zookeeper.add_broker(broker2, partition=0)

    # Simulate heartbeats
    zookeeper.send_heartbeat(broker1.id)
    zookeeper.send_heartbeat(broker2.id)

    # Verify that brokers are marked as up
    assert broker1.id in zookeeper.get_active_brokers()
    assert broker2.id in zookeeper.get_active_brokers()

    print("Test passed!")

def test_subscription_and_message_handling():
    # Initialize ZooKeeper (you can adapt this based on your actual setup)
    host_name = os.getenv("BROKER")
    zookeeper = ZooKeeper(host_name, 8000, 8888, 7500)

    # Simulate a subscriber
    # what to give subscriber id?
    subscriber_id = "subscriber1"

    # Subscribe to a broker
    zookeeper.choose_broker_for_subscription(subscriber_id)

    # Simulate receiving messages
    for i in range(5):
        message = f"Message {i}"
        broker = zookeeper.consume()  # Get a broker for message consumption
        broker.handle_message(message)  # Simulate handling the message


    print("Test passed!")

def test_message_consistency():
    # Initialize ZooKeeper (you can adapt this based on your actual setup)
    host_name = os.getenv("BROKER")
    zookeeper = ZooKeeper(host_name, 8000, 8888, 7500)

    # Simulate adding a broker and its replica
    broker = Broker(host_name, 9000, 9500, 1)
    zookeeper.add_broker(broker, partition=0)

    # Simulate sending a message to the broker
    message = "Hello, world!"
 #   broker.handle_client()

    # Retrieve the message from the replica using extract_message
  #  received_message = replica.extract_message(message)

    # Verify that the received message matches the original message
  #  assert received_message == message

  #  print("Test passed!")

if __name__ == "__main__":
  #  test_add_brokers_and_replicas()
  #  test_heartbeat_and_health_check()
 #   test_subscription_and_message_handling()
  #  test_message_consistency()
    asyncio.run(main())
