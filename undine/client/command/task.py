
class Task:
    _STATE = ['all', 'ready', 'issued', 'done', 'canceled', 'failed']

    _WHERE_CLAUSE = ['mid', 'name', 'cid', 'iid', 'wid', 'host', 'reportable']

    _DESC = {
        'list': '{} tasks lookup commands for the specific mid.',
        'info': 'Detail task information lookup command for specific tid.'
    }

    def __init__(self, config, connector):
        self._connector = connector
        self._config = config

    @staticmethod
    def help():
        return 'List-up specific tasks'

    @staticmethod
    def add_arguments(parser):
        subparsers = parser.add_subparsers(help='Task lookup commands',
                                           dest='sub_command')

        # Task list-up parser
        for state in Task._STATE:
            description = Task._DESC['list'].format(state.capitalize())

            task_list = subparsers.add_parser(state,
                                              description=description)

            task_list.add_argument('mid', help='Mission ID (MID)')

            task_list.add_argument('-n', '--name', dest='name',
                                   help='Task name', action='store')

            task_list.add_argument('-c', '--cid', dest='cid',
                                   help='Config ID (CID)', action='store')

            task_list.add_argument('-i', '--iid', dest='iid',
                                   help='Input ID (IID)', action='store')

            task_list.add_argument('-w', '--wid', dest='wid',
                                   help='Worker ID (WID)', action='store')

            task_list.add_argument('-H', '--host', dest='host',
                                   help='Hostname', action='store')

            task_list.add_argument('-r', '--reportable', dest='reportable',
                                   help='Reportable task', default='all',
                                   choices=['all', 'true', 'false'])

        # Task detail information parser
        task_info = subparsers.add_parser('info',
                                          description=Task._DESC['info'])

        task_info.add_argument('tid', help='Task ID (TID)')

    def run(self):
        if self._config.sub_command in self._STATE:
            where = {key: value
                     for key, value in vars(self._config).items()
                     if key in self._WHERE_CLAUSE and value}

            if self._config.sub_command != 'all':
                where['state'] = self._config.sub_command

            if where['reportable'] == 'all':
                del where['reportable']
            else:
                where['reportable'] = (where['reportable'] == 'true')

            print(self._connector.task_list(**where))

        elif self._config.sub_command == 'info':
            print(self._connector.task_info(self._config.tid))
