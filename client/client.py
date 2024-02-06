import requests
import json
import socket
from time import sleep
from typing import Callable

# zookeeper ips
zookeeper_ips = ["127.0.0.1"]
port = 8888
master_ip = None
client_socket = None
client_subscribe_socket = None


# find the master zookeeper
def find_master():
    for ip in zookeeper_ips:
        response = requests.get(f"{ip}/master")
        if response.status_code == 200:
            return ip
    return None


# open connection with server
def open_connection(node_ip, node_port):
    host = node_ip
    port = node_port

    new_socket = socket.socket()
    new_socket.connect((host, port))
    print("Connected to server")
    return new_socket


# push message to server -> post
def push_message(key: str, value: str):
    if client_socket is None:
        # TODO return error
        return None
    message = {
        "command": "push",
        "key": key,
        "value": value
    }

    client_socket.send(json.dumps(message).encode())
    data = client_socket.recv(1024).decode()
    print(f"Received from server: {data}")


# pull message from server -> get
def pull_message():
    if client_socket is None:
        # TODO return error
        return None
    message = {
        "command": "pull"
    }

    client_socket.send(json.dumps(message).encode())
    data = client_socket.recv(1024).decode()
    print(f"Received from server: {data}")


# subscribe to server
def subscribe(f: Callable):
    client_subscribe_socket = open_connection("127.0.0.1", port)
    # TODO: complete later


def main():
    global client_socket
    global master_ip
    # master_ip = find_master()
    # if master_ip is None:
    #     print("No master found")
    #     return
    while True:
        client_socket = open_connection("127.0.0.1", port)
        if client_socket is None:
            print("Error occured")

        push_message("Hello", "world")
        sleep(30)
    # client_socket.close()
