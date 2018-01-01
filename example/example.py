#!/usr/bin/env python3
from argparse import ArgumentParser, REMAINDER
from collections import namedtuple
from time import sleep
from random import randrange
from sys import stderr

import json
import os


class Example:
    """ Example script template to debug the Undine
    """

    ''' Error description variables.
    '''
    _FILE_NOT_EXIST = 'File not exists at {0}\n'
    _FILE_CREATE_FAIL = "Couldn't create file({0})"

    ''' Input argument data type and argument list.
    '''
    Option = namedtuple('Options',
                        ['short', 'long', 'dest', 'action', 'meta', 'help',
                         'required'])

    _OPTIONS = [
        Option('-c', '--config', 'config_file', 'store', 'PATH',
               'Config file path', True),
        Option('-r', '--result', 'result_file', 'store', 'PATH',
               'Result file path', True)
    ]

    def _listing_args(self):
        string = ['\n==========[ Program Arguments ]==========\n']

        string.extend(["{0}: {1}\n".format(k.upper(), v)
                       for k, v in vars(self._config).items()])

        return string

    def _listing_conf(self):
        string = ['\n========[ Config File Contents ]=========\n']

        try:
            json_string = json.load(open(self._config_file, 'r'))
            string.append(json.dumps(json_string, indent=4) + '\n')

        except IOError:
            string.append(self._FILE_NOT_EXIST.format(self._config_file))

        return string

    def _listing_input(self):
        string = ['\n=========[ Input File Contents ]=========\n']

        for val in self._files:
            string.append('------------[ File Contents ]------------\n')
            string.append('Filename: {0}\n'.format(val))

            if os.path.exists(val) and os.path.isfile(val):
                string.append('Contents:\n{0}\n'.format(open(val, 'r').read()))
            else:
                string.append(self._FILE_NOT_EXIST.format(val))

        return string

    @staticmethod
    def _parse(options = _OPTIONS):
        """ Arguments parsing method

        :param options: List of Example.Option tuple object.
        :return: Namespace object the parsed argument by ArgumentParser
        """
        parser = ArgumentParser(description='Example script for Undine.')

        for item in options:
            parser.add_argument(item.short, item.long, metavar=item.meta,
                                dest=item.dest, action=item.action,
                                help=item.help, required=item.required)

        parser.add_argument('files', metavar='FILE',
                            nargs=REMAINDER, help='Input _files')

        return parser.parse_args()

    def __init__(self):
        """ Constructor
        """
        self._config = self._parse()

        self._config_file = self._config.config_file
        self._result_file = self._config.result_file
        self._files = self._config.files

    def run(self):
        """ Task run method

        :return: Nothing
        """
        try:
            random_sleep = randrange(1, 10)

            with open(self._result_file, 'w') as f_out:
                f_out.write("Executed script: {0}\n".format(__file__))
                f_out.write("Working Directory: {0}\n".format(os.getcwd()))
                f_out.write("Random Sleep: {0}\n".format(random_sleep))
                f_out.writelines(self._listing_args())
                f_out.writelines(self._listing_conf())
                f_out.writelines(self._listing_input())

            sleep(random_sleep)

        except IOError:
            stderr.write(self._FILE_CREATE_FAIL.format(self._result_file))
            raise


if __name__ == '__main__':
    Example().run()
