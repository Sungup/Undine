from argparse import ArgumentParser
from undine.client.command import CommandFactory
from undine.client.connector import ConnectorFactory
from undine.client.view import ViewFactory
from undine.utils.exception import UndineException

import json
import os


class UndineClient:
    @staticmethod
    def parse():
        parser = ArgumentParser(description='Undine client tool.')

        # Global Argument
        parser.add_argument('-c', '--config', dest='config_file',
                            help='Config file path',
                            default='config/client.json',
                            action='store', metavar='PATH')

        parser.add_argument('-f', '--form', dest='form',
                            help='Print format',
                            choices=['json', 'table'], default='table')

        CommandFactory.add_subparsers(parser,
                                      dest='command',
                                      description='commands')

        return parser.parse_args()

    def __init__(self, sub_command, config, connection):
        conn = ConnectorFactory.create(connection)
        self._connector = ViewFactory.create(config.form, conn)

        self._command = CommandFactory.get_command(sub_command, config,
                                                   self._connector)

    def _run(self):
        self._command.run()

    @staticmethod
    def run():
        try:
            config = UndineClient.parse()

            if not os.path.isfile(config.config_file):
                raise UndineException('No such config file at {}.'.format(
                    config.config_file))

            connection = json.load(open(config.config_file, 'r'))
            sub_command = config.command

            # Remove useless global arguments
            #  - config_file: already used parsing json config file.
            del config.config_file
            del config.command

            UndineClient(sub_command, config, connection)._run()

        except UndineException as error:
            print(error.message)
            pass
