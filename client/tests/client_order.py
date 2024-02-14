import threading

import requests
import json
import socket
from time import sleep
from typing import Callable, Dict, List
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

zookeeper_ips = ["127.0.0.1"]
port = 8000
master_ip = None
client_socket = None
client_subscribe_socket = None

logging.log(logging.INFO, "Client started")

TEST_SIZE = 1000
KEY_SIZE = 8
SUBSCRIER_COUNT = 1

key_seq = [random.choice(range(KEY_SIZE)) for _ in range(TEST_SIZE)]

pulled: Dict[str, List[int]] = {}
for i in range(KEY_SIZE):
    pulled[f"{i}"] = []

def validate_pull(key: str, val: str):
    try:
        next_val = int(val)
        if len(pulled[key]) != 0:
            prev_val = pulled[key][-1]
            if prev_val >= next_val:
                print(f"order violation, seq: [{prev_val}, {next_val}]\tkey: [{key}]")
                sys.exit(255)
        pulled[key].append(next_val)
    except Exception as e:
        print(f"Exception: {e}")
        sys.exit(255)

# find the master zookeeper
def find_master():
    for ip in zookeeper_ips:
        response = requests.get(f"http://{ip}/zookeeper")
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
    message = {"type": "PUSH", "key": key, "value": value}

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
    if repr(data).startswith("Brokers"):
        print(repr(data))
        return
    host_b, port_b, part_no = data.split(",")
    port_b = int(port_b)
    message["part_no"] = int(part_no)

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
    }

    client_socket.send(json.dumps(message_0).encode())
    data = client_socket.recv(1024).decode()
    if repr(data).startswith("There is not"):
        print(repr(data))
        return
    host_b, port_b, par_no = data.split(",")
    port_b = int(port_b)
    client_subscribe_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_subscribe_socket.connect((host_b, port_b))
    message_1 = {"type": "SUBSCRIBE", "broker_id": "0", "part_no": par_no}
    client_subscribe_socket.send(json.dumps(message_1).encode())
    new_data = client_subscribe_socket.recv(1024).decode()

    print(f"Received from server: {repr(new_data)}")
    threading.Thread(target=receive_message, daemon=True, args=(f,)).start()


# a function to receive messages from the server (if we are subscribed)
# but if response doesn't come in 5 seconds, we continue with our work
def receive_message(f=None):
    global client_subscribe_socket
    client_subscribe_socket.settimeout(6)
    while True:
        try:
            data = client_subscribe_socket.recv(1024).decode()
            if not repr(data).startswith("No message"):
                if f is not None:
                    args = repr(data).replace("'", '"').strip()
                    args = str(args)
                    args = args.split("\\n")
                    # remove the elements which are empty strings
                    args = list(filter(lambda x: x != "" and x != "'" and x != '"', args))
                    if len(args) > 1:
                        for arg in args:
                            arg = arg.strip()
                            #remove the first \" and last \" from the string
                            if arg[0] == '"':
                                arg = arg[1:]
                            if arg[-1] == '"':
                                arg = arg[:-1]
                            arg = json.loads(arg)
                            data = f(arg["key"], arg["value"])
                    else:
                        arg = args[0].strip()
                        #remove the first \" and last \" from the string
                        if arg[0] == '"':
                            arg = arg[1:]
                        if arg[-1] == '"':
                            arg = arg[:-1]
                        arg = json.loads(arg)
                        data = f(arg["key"], arg["value"])
        except Exception as e:
            continue


async def main():
    global client_socket
    global master_ip

    host_name = os.getenv("ZOOKEEPER")
    client_socket = open_connection(host_name, port)

    for i in range(TEST_SIZE // 4):
        await push_message(f"{key_seq[i]}", f"{i}")

    for i in range(SUBSCRIER_COUNT):
        await subscribe(validate_pull)
    
    for i in range(TEST_SIZE // 4, TEST_SIZE):
        await push_message(f"{key_seq[i]}", f"{i}")

    sleep(10)
    print("order test passed successfully!")

if __name__ == "__main__":
    asyncio.run(main())