import enum

class STATUS(enum.Enum):
    OK = 0
    ERROR = 1
    WARNING = 2
    UNKNOWN = 3

class SOCKET_STATUS(enum.Enum):
    WRITE_SUCCESS = {"status": "OK", "message": "Write successful"}
    WRITE_FAILED = {"status": "ERROR", "message": "Write failed"}