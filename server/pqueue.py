import queue

"""
partition queue
it keeps a record of messages and other probable things
"""


class Pqueue:
    def __init__(self, part_no, is_replica) -> None:
        self.part_no = part_no
        self.is_replica = is_replica
        self.queue = []

    def __str__(self):
        return f"Pqueue(part_no={self.part_no}, is_replica={self.is_replica}, queue={self.queue})"

    # checks if the current node is not replica and can be read from
    def is_eligible_to_read(self):
        return not self.is_replica

    def read(self):
        if self.is_eligible_to_read():
            if len(self.queue) == 0:
                # Error
                return None

            message = self.queue.pop(0)  # Get the last message and remove it
            print(f"Reading message: {message}")
            return message

    def mark_as_read(self):
        if self.is_eligible_to_read() and len(self.queue) > 0:
            self.queue.pop()  # Remove the last message
            print(f"Marked last message as read")

    def write_new_message(self, message):
        if self.is_replica:
            # Error
            return 0
        print(f"Writing new message: {message}")
        self.write(message)
        return 1

    def become(self, new_state):
        if new_state == "replica":
            if self.is_replica:
                # Error
                return "already replica"
            self.is_replica = True
        else:
            if not self.is_replica:
                # Error
                return "already primary"
            self.is_replica = False

    def write(self, message):
        self.queue.append(message)
        return 1

    def replicate(self, arr_message):
        if self.is_replica:
            self.queue = self.queue + arr_message
            return 1

        # not a replica
        return 0
