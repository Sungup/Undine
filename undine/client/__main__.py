from argparse import ArgumentParser
from collections import namedtuple
from undine.utils.exception import UndineException

import json
import os


class ConfigParser:
    Option = namedtuple('Options',
                        ['short', 'long', 'dest', 'action', 'meta', 'help',
                         'required'])

    _OPTIONS = [
        Option('-c', '--config', 'config_file', 'store', 'PATH',
               'Config file path', True)
    ]

    @staticmethod
    def parse(options = _OPTIONS):
        # TODO Add multi level parser
        parser = ArgumentParser(description='Undine task manager.')

        for item in options:
            parser.add_argument(item.short, item.long, metavar=item.meta,
                                dest=item.dest, action=item.action,
                                help=item.help, required=item.required)

        return parser.parse_args()

    @staticmethod
    def load_config():
        config = ConfigParser.parse()

        if not os.path.isfile(config.config_file):
            raise UndineException('No such config file at {}.'.format(
                config.config_file))

        return json.load(open(config.config_file, 'r'))


class UndineClient:
    def __init__(self, config):
        if 'database' not in config:
            raise UndineException('There is no database connection information')

        if 'rpc' not in config:
            raise UndineException('There is no rpc connection information')

        self.driver = config['database']
        self.rpc = config['rpc']

    def _run(self):
        print(str(self.driver))
        print(str(self.rpc))

    @staticmethod
    def run():
        UndineClient(ConfigParser.load_config())._run()


if __name__ == '__main__':
    UndineClient.run()
