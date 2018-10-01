from collections import namedtuple
from argparse import ArgumentParser
from undine.setup.generator import __setup_env__ as env

import json


def add_arguments(parser):
    __Option = namedtuple('__Option', ('name', 'default', 'type'))

    _sub_parser = parser.add_subparsers(help='Configuration type',
                                        dest='config_type')

    for name, config in env.__DEFAULT_CONFIG.items():
        _item = _sub_parser.add_parser(name)

        for options in config:
            options.add_argument(_item)

        _item.add_argument('config_dest', type=str,
                           metavar='PATH', action='store',
                           help='Destination file path')


def run(**kwargs):
    config_type = kwargs['config_type'].lower()
    config_dest = kwargs['config_dest']

    del kwargs['config_type']
    del kwargs['config_dest']

    contents = json.dumps({o.name: o.parse_args(**kwargs)
                           for o in env.__DEFAULT_CONFIG[config_type]},
                          indent=2)

    if config_dest == 'stdout':
        print(contents)
    else:
        with open(config_dest, 'w') as f_out:
            f_out.write(contents)


def main():
    _parser = ArgumentParser(description='Config file generator')

    add_arguments(_parser)

    run(**vars(_parser.parse_args()))
