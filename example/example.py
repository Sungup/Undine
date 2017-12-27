#!/usr/bin/env python3


from argparse import ArgumentParser
import os
import json


class Example:
    __OPTIONS__ = {
        'config': ['-c', '--config', 'config_path', 'store',
                   'Config file path'],
        'result': ['-r', '--result', 'result_path', 'store',
                   'Result file path']
    }

    def __init__(self, args):
        self.config = self.__parse__(args)
        self.args = args

    @staticmethod
    def __parse__(args):
        parser = ArgumentParser(description='Example script for Undine.')

        for item in Example.__OPTIONS__:
            parser.add_argument(item[0], item[1],
                                dest=item[2], action=item[3], help=item[4])

        return parser.parse_args(args)

    def run(self):
        with open(self.args.result_path, 'w') as f_out:
            f_out.write("Executed script: {}\n".format(__file__))
            f_out.write("Working Directory: {}\n".format(os.getcwd()))
            f_out.writelines(self.__listing_args__())
            f_out.writelines(self.__listing_conf__())
            f_out.writelines(self.__listing_input__())

    def __listing_args__(self):
        string = ["---------- Program Arguments ----------"]

        for key, value in vars(self.config).items():
            string.append("{0}: {1}".format(key.upper(), value))

        return string

    def __listing_conf__(self):
        json.load(open(self.config.config_path, 'r'))
        return []

    def __listing_input__(self):
        return []