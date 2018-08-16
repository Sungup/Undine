from undine.client.command.base_command import BaseCommand
from undine.utils.exception import UndineException

import json


class Host(BaseCommand):
    _DESC = {
        'list': 'Host lists and its task status lookup command.',
        'state': 'Host state lookup command (supports json format only).'
    }

    def __init__(self, config, connector):
        super(self.__class__, self).__init__(config, connector)

    #
    # Private methods
    #
    def _rpc_call(self, ip, command, *args, **kwargs):
        state = self._connector.rpc_call(ip, command, *args, **kwargs)

        return json.dumps(json.loads(state), indent=4)

    #
    # Public methods
    #
    @staticmethod
    def help():
        return 'Host lookup command'

    @staticmethod
    def add_arguments(parser):
        subparsers = parser.add_subparsers(help='Host lookup commands',
                                           dest='sub_command')

        # Host list's parser
        subparsers.add_parser('list', description=Host._DESC['list'])

        # Host status query
        host_state = subparsers.add_parser('state',
                                           description=Host._DESC['state'])

        host_state.add_argument('host', help='Host IP (not hostname)')

        host_state.add_argument('-l', '--list', dest='command_list',
                                help='Check available commands.',
                                action='store_true')

        host_state.add_argument('-c', '--command', dest='rpc_command',
                                help='Host state command',
                                default='all', action='store')

    def run(self):
        if self._config.sub_command == 'list':
            print(self._connector.host_list())
        elif self._config.sub_command == 'state' and self._config.command_list:
            print(self._rpc_call(self._config.host, 'list'))
        elif self._config.sub_command == 'state':
            print(self._rpc_call(self._config.host, self._config.rpc_command))
        else:
            raise UndineException("Unsupported command")
