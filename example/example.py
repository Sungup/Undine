#!/usr/bin/env python3
from argparse import ArgumentParser, REMAINDER
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

    def _listing_input_files(self):
        string = ['\n=========[ Input File Contents ]=========\n']

        for val in self._inputs:
            string.append('------------[ File Contents ]------------\n')
            string.append('Filename: {0}\n'.format(val))

            if os.path.exists(val) and os.path.isfile(val):
                string.append('Contents:\n{0}\n'.format(open(val, 'r').read()))
            else:
                string.append(self._FILE_NOT_EXIST.format(val))

        return string

    def _listing_input_ids(self):
        string = ['\n============[ Input ID List ]============\n']

        string += [' - FileID: {}\n'.format(id_) for id_ in self._inputs]

        return string

    @staticmethod
    def _parse():
        """ Arguments parsing method

        :return: Namespace object the parsed argument by ArgumentParser
        """
        parser = ArgumentParser(description='Example script for Undine.')

        parser.add_argument('--type', dest='input_type',
                            action='store', choices=['file', 'id'],
                            default='file', help='Input argument type')

        parser.add_argument('-c', '--config', dest='config_file',
                            action='store', metavar='PATH',
                            help='Config file path', required=True)

        parser.add_argument('-r', '--result', dest='result_file',
                            action='store', metavar='PATH',
                            help='Result file path', required=True)

        parser.add_argument('inputs', metavar='INPUTS',
                            nargs=REMAINDER, help='List of input files or IDs')

        return parser.parse_args()

    def __init__(self):
        """ Constructor
        """
        self._config = self._parse()

        if self._config.input_type == 'file':
            self._listing_input = self._listing_input_files
        else:
            self._listing_input = self._listing_input_ids

        self._config_file = self._config.config_file
        self._result_file = self._config.result_file
        self._inputs = self._config.inputs

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
