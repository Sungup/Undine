from __future__ import print_function

import multiprocessing
import platform
import subprocess
import sys


class System:
    _MACOS_DQ_QUERY = (('route', '-n', 'get', 'default'),
                       ('awk', '/gateway/ { print $2 }'))

    _LINUX_GW_QUERY = (('ip', 'route', 'show', 'default'),
                       ('awk', '/default/ { print $3 }'))

    '''
    _WINDOW_GW_QUERY = '@for /f "token=3" %j in ' \
                            '(\'route print ^|findstr "\\<0.0.0.0\\>"\') ' \
                            'do @echo %j'
    '''

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
    def default_gateway():
        # Select system dependent gw query string
        if System.is_window():
            raise Exception('Currently windows platform not in support')
        elif System.is_mac():
            query = System._MACOS_DQ_QUERY
        else:
            query = System._LINUX_GW_QUERY

        # Query the network device
        net_query = subprocess.Popen(query[0], stdout=subprocess.PIPE)

        # After processing to grepping the default gw address
        output = subprocess.check_output(query[1], stdin=net_query.stdout)

        net_query.wait()

        return output.strip()


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def print_console_header(string, splitter='-'):
    str_size = len(string) + 2
    left_size = 80 - str_size
    right_size = 80 - left_size - str_size

    return '{1} {0} {2}'.format(string, splitter*left_size, splitter*right_size)
