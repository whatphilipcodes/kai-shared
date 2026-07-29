import logging
from enum import IntEnum, StrEnum

class LogLevel(IntEnum):
    CRITICAL = logging.CRITICAL
    FATAL = logging.FATAL
    ERROR = logging.ERROR
    WARNING = logging.WARNING
    WARN = logging.WARN
    INFO = logging.INFO
    DEBUG = logging.DEBUG
    NOTSET = logging.NOTSET


class NetworkProtocol(StrEnum):
    TCP = "tcp://"
    IPC = "ipc://"
