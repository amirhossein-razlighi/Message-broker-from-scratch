import pickle
import socket

from server.broker import Broker
from server.pqueue import Pqueue


class Replica(Broker):
    def __init__(self, host, port):
        super().__init__(host, port)
        self._is_replica = True

    def __str__(self):
        return f"Replica(id={self.id}, pqueues={self._pqueues}, is_replica={self._is_replica})"

    def send_message(self, broker, message):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((broker.host, broker.port))
            s.sendall(pickle.dumps(message))
            data = s.recv(1024)
        return pickle.loads(data)

    def handle_message(self, message):
        if message['type'] == 'update':
            part_no = message['part_no']
            message = message['message']

            if part_no in self._pqueues:
                print(f"Writing new message to existing queue {part_no}...")
                self._pqueues[part_no].write_new_message(message)
            else:
                print(f"Creating new queue {part_no} and writing new message...")
                self._pqueues[part_no] = Pqueue(part_no, is_replica=True)
                self._pqueues[part_no].write_new_message(message)

            return 'Update successful'

    def update(self, broker, part_no, message):
        print(f"Replica {self.id} updating...")
        if broker.id != self.id:
            message = {'type': 'update', 'part_no': part_no, 'message': message}
            response = self.send_message(broker, message)
            print(response)
