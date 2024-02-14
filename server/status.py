import enum


class STATUS(enum.Enum):
    OK = 0
    ERROR = 1
    WARNING = 2
    UNKNOWN = 3
    SUCCESS = 4


class SOCKET_STATUS(enum.Enum):
    WRITE_SUCCESS = {"status": "OK", "message": "Write successful"}
    WRITE_FAILED = {"status": "ERROR", "message": "Write failed"}
    SUBSCRIBE_SUCCESS = {"status": "OK", "message": "Subscribe successful"}
    SUBSCRIBE_FAILED = {"status": "ERROR", "message": "Subscribe failed"}
    PARTITION_SUCCESS = {"status": "OK", "message": "partition added successfully"}
    REPLICA_CONSISTENCY_SUCCESS = {"status": "OK", "message": "Replica consistency update successful"}
    REPLICA_CONSISTENCY_FAILED = {"status": "ERROR", "message": "Replica consistency update failed"}