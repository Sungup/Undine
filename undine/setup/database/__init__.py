from undine.setup.database.__mariadb__ import MariaDBInitializer
from undine.utils.exception import UndineException
from argparse import ArgumentParser

import json


def add_arguments(parser):
    _default = '/etc/aria/undine.json'

    parser.add_argument(
        'config_file', metavar='PATH',
        action='store', default=_default,
        help='Config file path [default: {}]'.format(_default)
    )


def run(**kwargs):
    _config = json.load(open(kwargs['config_file'], 'r'))

    # Check configuration info
    if 'driver' not in _config and 'database' not in _config:
        raise UndineException("DB information doesn't exist")

    _key = 'driver' if 'driver' in _config else 'database'

    if 'type' not in _config[_key] or _config[_key]['type'] != 'mariadb':
        raise UndineException('Configuration type is not supported.')

    _initializer = MariaDBInitializer(_config)

    _initializer.build_table()
    _initializer.load_initial_data()
    _initializer.post_initialization()


def main():
    _parser = ArgumentParser(description='Database initializer')

    add_arguments(_parser)

    run(**vars(_parser.parse_args()))
