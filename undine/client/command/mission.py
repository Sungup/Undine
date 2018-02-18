
class Mission:
    _DESC = {
        'list': 'Mission lists and tasks status lookup command.',
        'info': 'Detail mission information lookup command.'
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
        mission_list = subparsers.add_parser('list',
                                             description=Mission._DESC['list'])

        mission_list.add_argument('-a', '--all', dest='all',
                                  help='List all missions', action='store_true')

        # Mission detail information parser
        mission_info = subparsers.add_parser('info',
                                             description=Mission._DESC['info'])

        mission_info.add_argument('-m', '--mid', dest='mid',
                                  help='Mission ID (mid)', action='store')

        mission_info.add_argument('-n', '--name', dest='name',
                                  help='Mission name', action='store')

        mission_info.add_argument('-e', '--email', dest='email',
                                  help='Email address of issuer',
                                  action='store')

    def run(self):
        if self._config.sub_command == 'list':
            print(self._connector.mission_list(self._config.all))
        elif self._config.sub_command == 'info':
            where = {key: value
                     for key, value in vars(self._config).items()
                     if key in ['mid', 'name', 'email'] and value}

            print(self._connector.mission_info(**where))
