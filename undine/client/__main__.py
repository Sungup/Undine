from argparse import ArgumentParser
from undine.utils.exception import UndineException

import json
import os


class ConfigParser:
    @staticmethod
    def parse():
        parser = ArgumentParser(description='Undine client tool.')

        # Global Argument
        parser.add_argument('-c', '--config', dest='config_file',
                            help='Config file path',
                            default='config/client.json',
                            action='store', metavar='PATH')

        parser.add_argument('-j', '--json', dest='json',
                            help='Print json format',
                            default=False, action='store_true')

        subparsers = parser.add_subparsers(help='commands')

        # Dashboard command
        dashboard = subparsers.add_parser('dashboard', help='Task dashboard')

        dashboard.add_argument('-t', '--term', dest='term',
                               help='Dashboard refresh term', default=10,
                               action='store', metavar='sec')

        # Missions command
        missions = subparsers.add_parser('missions', help='Query task missions')

        missions.add_argument('-m', '--mid', dest='mid',
                              help="Mission ID (mid)", action='store')

        # Tasks command
        tasks = subparsers.add_parser('tasks', help='List-up specific tasks')

        tasks.add_argument('-t', '--tid', dest='tid',
                           help="Task id (tid)", action='store')

        # Nodes command
        nodes = subparsers.add_parser('hosts', help='List-up service nodes')

        # Stats command
        stats = subparsers.add_parser('stats', help='Service node stats')

        # TODO Add additional parser

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
