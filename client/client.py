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
    # client_socket = open_connection("127.0.0.1", 8000)
    # while True:
    #     if client_socket is None:
    #         print("Error occured")

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



if __name__ == "__main__":
    main()
