
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


class Config:
    def __init__(self, config, connector):
        self._connector = connector
        self._config = config

    @staticmethod
    def help():
        return 'Lookup configuration'

    @staticmethod
    def add_arguments(parser):
        parser.add_argument('cid', help='Config ID (CID)')

    def run(self):
        print(self._connector.config_info(self._config.cid))


class Input:
    _DESC = {
        'list': 'Input list lookup command.',
        'info': 'Input information lookup command.'
    }

    def __init__(self, config, connector):
        self._connector = connector
        self._config = config

    @staticmethod
    def help():
        return 'Lookup input parameters'

    @staticmethod
    def add_arguments(parser):
        subparsers = parser.add_subparsers(help='Input lookup commands',
                                           dest='sub_command')

        # Input list's parser
        subparsers.add_parser('list', description=Input._DESC['list'])

        # Input detail information parser
        input_info = subparsers.add_parser('info',
                                           description=Input._DESC['info'])

        input_info.add_argument('iid', help='Input ID (IID)')

    def run(self):
        if self._config.sub_command == 'list':
            print(self._connector.input_list())
        else:
            print(self._connector.input_info(self._config.iid))


class Worker:
    _DESC = {
        'list': 'Input list lookup command.',
        'info': 'Input information lookup command.'
    }

    def __init__(self, config, connector):
        self._connector = connector
        self._config = config

    @staticmethod
    def help():
        return 'Lookup worker information'

    @staticmethod
    def add_arguments(parser):
        subparsers = parser.add_subparsers(help='Worker lookup commands',
                                           dest='sub_command')

        # Worker list's parser
        subparsers.add_parser('list', description=Worker._DESC['list'])

        # Worker detail information parser
        worker_info = subparsers.add_parser('info',
                                            description=Worker._DESC['info'])

        worker_info.add_argument('wid', help='Worker ID (WID)')

    def run(self):
        if self._config.sub_command == 'list':
            print(self._connector.worker_list())
        else:
            print(self._connector.worker_info(self._config.wid))
