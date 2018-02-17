from curses import wrapper, use_default_colors, curs_set
from datetime import datetime
from time import sleep
from undine.utils.exception import UndineException
from undine.utils.system import System

import curses


class Dashboard:
    _TITLE = 'Undine Task Manager Dashboard'

    _HEADER = ('MID', 'Name', 'Email',
               'Ready', 'Issued', 'Done', 'Canceled', 'Failed', 'Issued')

    def __init__(self, config, connector):
        self._connector = connector
        self._term = config.term

        if config.json:
            raise UndineException("Dashboard doesn't support json option")

        host = System.host_info()
        self._title = '{title} ({ver})'.format(title=Dashboard._TITLE,
                                               ver=System.version())

        self._host = '{host} ({ip})'.format(host=host.name, ip=host.ipv4)

    @staticmethod
    def help():
        return 'Task dashboard'

    @staticmethod
    def add_arguments(parser):
        parser.add_argument('-t', '--term', dest='term', type=int, default=10,
                            help='Dashboard refresh term [default: 10]',
                            action='store', metavar='sec')

    def run(self):
        wrapper(self.main)

    def main(self, screen):
        try:
            # Clear background and remove blinking cursor
            use_default_colors()
            curs_set(0)

            screen.clear()

            while True:
                datetime_string = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                screen.erase()

                # Print header information
                screen.addstr(0, 0, self._title, curses.A_BOLD)
                screen.addstr(1, 1, '- Datetime: ' + datetime_string)
                screen.addstr(2, 1, '- Localhost: ' + self._host)

                # Print table
                screen.addstr(3, 0, self._connector.mission_list())

                screen.refresh()
                sleep(self._term)

        except KeyboardInterrupt:
            pass
