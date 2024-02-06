import socket
import json
import time


def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('127.0.0.1', 8888))
        message = {"key": "hi", "value": "hello", "type": "PUSH"}
        s.sendall(json.dumps(message).encode())
        data = s.recv(1024)

    print('Received', repr(data))


if __name__ == '__main__':
    main()
