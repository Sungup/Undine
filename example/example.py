#!/usr/bin/env python3
from argparse import ArgumentParser
from collections import namedtuple

import argparse
import json
import os
import sys


class Example:
    """ Example script template to debug the Undine
    """

    ''' Error description variables.
    '''
    __file_not_exist__ = 'File not exists at {0}\n'
    __file_create_fail__ = "Couldn't create file({0})"

    ''' Input argument data type and argument list.
    '''
    Option = namedtuple('Options',
                        ['short', 'long', 'dest', 'action', 'meta', 'help',
                         'required'])

    __OPTIONS__ = [
        Option('-c', '--config', 'config_file', 'store', 'PATH',
               'Config file path', True),
        Option('-r', '--result', 'result_file', 'store', 'PATH',
               'Result file path', True)
    ]

    def __listing_args__(self):
        string = ['\n==========[ Program Arguments ]==========\n']

        string.extend(["{0}: {1}\n".format(k.upper(), v)
                       for k, v in vars(self.config).items()])

        return string

    def __listing_conf__(self):
        string = ['\n========[ Config File Contents ]=========\n']

        try:
            json_string = json.load(open(self.config_file, 'r'))
            string.append(json.dumps(json_string, indent=4) + '\n')

        except IOError:
            string.append(self.__file_not_exist__.format(self.config_file))

        return string

    def __listing_input__(self):
        string = ['\n=========[ Input File Contents ]=========\n']

        for val in self.files:
            string.append('------------[ File Contents ]------------\n')
            string.append('Filename: {0}\n'.format(val))

            if os.path.exists(val) and os.path.isfile(val):
                string.append('Contents:\n{0}\n'.format(open(val, 'r').read()))
            else:
                string.append(self.__file_not_exist__.format(val))

        return string

    @staticmethod
    def __parse__(options = __OPTIONS__):
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
                            nargs=argparse.REMAINDER, help='Input files')

        return parser.parse_args()

    def __init__(self):
        """ Constructor
        """
        self.config = self.__parse__()

        print(self.config)

        self.config_file = self.config.config_file
        self.result_file = self.config.result_file
        self.files = self.config.files

    def run(self):
        """ Task run method

        :return: Nothing
        """
        try:
            with open(self.result_file, 'w') as f_out:
                f_out.write("Executed script: {0}\n".format(__file__))
                f_out.write("Working Directory: {0}\n".format(os.getcwd()))
                f_out.writelines(self.__listing_args__())
                f_out.writelines(self.__listing_conf__())
                f_out.writelines(self.__listing_input__())

        except IOError:
            sys.stderr.write(self.__file_create_fail__.format(self.result_file))
            raise


if __name__ == '__main__':
    Example().run()
