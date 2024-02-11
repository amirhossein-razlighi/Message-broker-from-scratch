import requests
import json
import socket
from time import sleep
from typing import Callable
import os
import argparse
import asyncio
import logging

# zookeeper ips
from server.broker import Broker
from server.replica import Replica
from server.zookeeper import ZooKeeper

zookeeper_ips = ['127.0.0.1']
port = 8000
master_ip = None
client_socket = None
client_subscribe_socket = None

logging.log(logging.INFO, "Client started")
# find the master zookeeper
def find_master():
    for ip in zookeeper_ips:
        response = requests.get(f"{ip}/master")
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

# push message to server
def push_message(key: str, value: str):
    if client_socket is None:
        # TODO return error
        return None
    message = {
        "type": "PUSH",
        "key": key,
        "value": value,
        "part_no": "0"
    }

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
    print(f"Received from server: {repr(data)}")

# subscribe to server
async def subscribe(f: Callable):
    # client_subscribe_socket = open_connection("127.0.0.1", port)
    if client_socket is None:
        # TODO return error
        return None
    message = {
        "type": "SUBSCRIBE",
        "broker_id": "0"
    }
    client_socket.send(json.dumps(message).encode())
    data = client_socket.recv(1024).decode()
    print(f"Received from server: {repr(data)}")

def main():
    global client_socket
    global master_ip
    # master_ip = find_master()
    # if master_ip is None:
    #     print("No master found")
    #     return
    host_name = os.getenv("BROKER")
    client_socket = open_connection(host_name, port)
    # client_socket = open_connection('127.0.0.1', port)

    while True:
        if client_socket is None:
            print("Error occured")

    #     push_message("Hello", "world")
    #     sleep(5)

    """
    loop = asyncio.get_event_loop()
    loop.run_until_complete(subscribe(None))

    sleep(6)

    for i in range(10):
        push_message(f"{i}", f"world {i}")
        push_message(f"{i}", f"world {i + 1}")
        push_message(f"{i}", f"world {i + 2}")
    
    sleep(12)
    for i in range(10):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(pull_message())

    client_socket.close()
    """


# Example test scenarios in the client

def test_add_brokers_and_replicas():
    # Initialize ZooKeeper (you can adapt this based on your actual setup)
    zookeeper = ZooKeeper("localhost", 8000, 8888, 7500)

    # Simulate adding brokers
    broker1 = Broker("broker1", 9000, 9500, 1)
    broker2 = Broker("broker2", 9001, 9501, 2)
    zookeeper.add_broker(broker1, partition=0)
    zookeeper.add_broker(broker2, partition=0)

    # Verify that brokers are correctly processed
    assert broker1.id in zookeeper.get_active_brokers()
    assert broker2.id in zookeeper.get_active_brokers()

    print("Test passed!")


def test_heartbeat_and_health_check():
    # Initialize ZooKeeper (you can adapt this based on your actual setup)
    zookeeper = ZooKeeper("localhost", 8000, 8888, 7500)

    # Simulate sending heartbeats from brokers
    broker1 = Broker("broker1", 9000, 9500, 1)
    broker2 = Broker("broker2", 9001, 9501, 2)
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
    zookeeper = ZooKeeper("localhost", 8000, 8888, 7500)

    # Simulate a subscriber
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
    zookeeper = ZooKeeper("localhost", 8000, 8888, 7500)

    # Simulate adding a broker and its replica
    broker = Broker("broker1", 9000, 9500, 1)
    replica = Replica("replica1", 9002)
    zookeeper.add_broker(broker, partition=0)

    # Simulate sending a message to the broker
    message = "Hello, world!"
    broker.handle_message(message)

    # Retrieve the message from the replica
    received_message = replica.get_message()

    # Verify that the received message matches the original message
    assert received_message == message

    print("Test passed!")
if __name__ == "__main__":
    test_add_brokers_and_replicas()
    test_heartbeat_and_health_check()
    test_subscription_and_message_handling()
    test_message_consistency()
    main()
