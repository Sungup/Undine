from argparse import ArgumentParser
from collections import namedtuple
from undine.utils.exception import UndineException

import undine.setup.generator as generator
import undine.setup.database as database


def main():
    _SubCmd = namedtuple('_SubCmd', ('desc', 'module'))

    _commands = {
        'gen-conf': _SubCmd('Generate a configuration file', generator),
        'db-init': _SubCmd('Initialize the database', database),
    }

    _parser = ArgumentParser(description='Undine setup tool.')

    _sub_parser = _parser.add_subparsers(help='Sub commands',
                                         dest='sub_command')

    # Add generator sub-parser
    for _cmd, _info in _commands.items():
        _info.module.add_arguments(
            _sub_parser.add_parser(_cmd, description=_info.desc)
        )

    # Parsing arguments
    _config = _parser.parse_args()

    _cmd = _config.sub_command
    del _config.sub_command

    if _cmd not in _commands:
        raise UndineException('{} is not supported command'.format(_cmd))

    _commands[_cmd].module.run(**vars(_config))


if __name__ == '__main__':
    main()
