
class Mission:
    _DESC = {
        'list': 'Mission lists and tasks status lookup command.',
        'info': 'Detail mission information lookup command.',
        'cancel': 'Cancel all tasks not yet started.'
    }

    def __init__(self, config, connector):
        self._connector = connector
        self._config = config

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

        _info.add_argument('-m', '--mid', dest='mid',
                           help='Mission ID (mid)', action='store')

        _info.add_argument('-n', '--name', dest='name',
                           help='Mission name', action='store')

        _info.add_argument('-e', '--email', dest='email',
                           help='Email address of issuer',
                           action='store')

        # Mission cancel
        _cancel = subparsers.add_parser('cancel',
                                        description=Mission._DESC['cancel'])

        _cancel.add_argument('-m', '--mid', dest='mid',
                             help='Mission ID (mid)', action='store')

    def run(self):
        if self._config.sub_command == 'list':
            print(self._connector.mission_list(self._config.all))

        else:
            where = {key: value
                     for key, value in vars(self._config).items()
                     if key in ['mid', 'name', 'email'] and value}

            if self._config.sub_command == 'info':
                print(self._connector.mission_info(**where))

            elif self._config.sub_command == 'cancel':
                number_of_task = self._connector.cancel_tasks(**where)
                print('{} tasks has been canceled'.format(number_of_task))
