import socket
import json
import time
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host to connect to")
    parser.add_argument("--port", type=int, default=8888, help="Port to connect to")
    args = parser.parse_args()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((args.host, args.port))
        message = {"key": "wow", "value": "bye", "type": "PUSH"}
        s.sendall(json.dumps(message).encode())
        data = s.recv(1024)

    print('Received', repr(data))


if __name__ == '__main__':
    main()
