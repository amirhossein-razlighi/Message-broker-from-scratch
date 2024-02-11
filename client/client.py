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

executor = ThreadPoolExecutor(max_workers=1)

# zookeeper ips
zookeeper_ips = ["127.0.0.1"]
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
    message = {"type": "PULL"}

    client_socket.send(json.dumps(message).encode())
    data = client_socket.recv(1024).decode()
    print(f"Received from server: {repr(data)}")


async def subscribe(f: Callable):
    if client_socket is None:
        # TODO return error
        return None
    message = {"type": "SUBSCRIBE", "broker_id": "0"}

    client_socket.send(json.dumps(message).encode())
    data = client_socket.recv(1024).decode()
    print(f"Received from server: {repr(data)}")
    threading.Thread(target=receive_message, daemon=True).start()


# a function to receive messages from the server (if we are subscribed)
# but if response doesn't come in 5 seconds, we continue with our work
def receive_message():
    client_socket.settimeout(3)
    while True:
        try:
            data = client_socket.recv(1024).decode()
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
    # host_name = os.getenv("BROKER")
    host_name = "127.0.0.1"
    # client_socket = open_connection(host_name, port)

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


if __name__ == "__main__":
    asyncio.run(main())
