from server.broker import Broker
from server.pqueue import Pqueue


class Replica(Broker):
    def __init__(self, host, port):
        super().__init__(host, port)
        self._is_replica = True
    def __str__(self):
        return f"Replica(id={self.id}, pqueues={self._pqueues}, is_replica={self._is_replica})"
    def update(self, broker, part_no, message):
        print(f"Replica {self.id} updating...")
        if broker != self:
            if part_no in self._pqueues:
                print(f"Writing new message to existing queue {part_no}...")
                self._pqueues[part_no].write_new_message(message)
            else:
                print(f"Creating new queue {part_no} and writing new message...")
                self._pqueues[part_no] = Pqueue(part_no, is_replica=True)
                self._pqueues[part_no].write_new_message(message)