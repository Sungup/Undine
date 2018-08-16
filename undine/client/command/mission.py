from undine.client.command.base_command import BaseCommand

import re


class Mission(BaseCommand):
    _DESC = {
        'list': 'Mission lists and tasks status lookup command.',
        'info': 'Detail mission information lookup command.',
        'cancel': 'Cancel all tasks not yet started.',
        'truncate': 'Remove all tasks in selected mission.',
        'remove': 'Remove all tasks and mission information.',
        'rerun': 'Rerun all tasks in selected mission.'
    }

    def __init__(self, config, connector):
        super(self.__class__, self).__init__(config, connector)

    @staticmethod
    def help():
        return 'Task mission lookup command'

    @staticmethod
    def add_arguments(parser):
        subparsers = parser.add_subparsers(help='Mission lookup commands',
                                           dest='sub_command')

        # Mission list's parser
        _list = subparsers.add_parser('list',
                                      description=Mission._DESC['list'])

        _list.add_argument('-a', '--all', dest='all',
                           help='List all missions', action='store_true')

        # Mission detail information parser
        _info = subparsers.add_parser('info',
                                      description=Mission._DESC['info'])

        _info.add_argument('identifier', help='Mission ID or name',
                           action='store')

        # Mission cancel
        _cancel = subparsers.add_parser('cancel',
                                        description=Mission._DESC['cancel'])
        _cancel.add_argument('identifier', help='Mission ID or name',
                             action='store')

        # Truncate mission
        _trunc = subparsers.add_parser('truncate',
                                       description=Mission._DESC['truncate'])
        _trunc.add_argument('identifier', help='Mission ID or name',
                            action='store')

        # Mission delete
        _remove = subparsers.add_parser('remove',
                                        description=Mission._DESC['remove'])
        _remove.add_argument('identifier', help='Mission ID or name',
                             action='store')

        # Rerun all tasks in mission
        _rerun = subparsers.add_parser('rerun',
                                       description=Mission._DESC['rerun'])
        _rerun.add_argument('identifier', help='Mission ID or name',
                            action='store')

    def run(self):
        if self._config.sub_command == 'list':
            print(self._connector.mission_list(self._config.all))

        else:
            if not re.match(r'^(?:[0-9A-Fa-f]){32}$', self._config.identifier):
                where = {'name': self._config.identifier}
            else:
                where = {'mid': self._config.identifier}

            if self._config.sub_command == 'info':
                print(self._connector.mission_info(**where))

            elif self._config.sub_command == 'cancel':
                total_tasks = self._connector.cancel_tasks(**where)
                print('{} tasks has been canceled.'.format(total_tasks))

            elif self._config.sub_command == 'truncate':
                total_tasks = self._connector.drop_tasks(**where)
                print('{} tasks has been removed.'.format(total_tasks))

            elif self._config.sub_command == 'remove':
                total_tasks = self._connector.drop_mission(**where)
                print('{} tasks has been removed.'.format(total_tasks))

            elif self._config.sub_command == 'rerun':
                total_tasks = self._connector.rerun_tasks(**where)
                print('{} tasks has been restart.'.format(total_tasks))
