from __future__ import print_function
from collections import namedtuple

import multiprocessing
import platform
import socket
import sys


class System:
    @staticmethod
    def is_window():
        return platform.system() == 'Windows'

    @staticmethod
    def is_linux():
        return platform.system() == 'Linux'

    @staticmethod
    def is_mac():
        return platform.system() == 'Darwin'

    @staticmethod
    def cpu_cores():
        return multiprocessing.cpu_count()

    @staticmethod
    def host_info():
        HostInfo = namedtuple('HostInfo', ['name', 'ipv4'])

        name = socket.gethostname()
        return HostInfo(name, socket.gethostbyname(name))

    @staticmethod
    def version():
        return 'v0.1.0b'


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def print_console_header(string, splitter='-'):
    str_size = len(string) + 2
    left_size = (80 - str_size) // 2
    right_size = 80 - left_size - str_size

    return '{1} {0} {2}'.format(string, splitter*left_size, splitter*right_size)
