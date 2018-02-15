
class Missions:
    _LIST_HEADER = ('MID', 'Name', 'Email',
                    'Ready', 'Issued', 'Done', 'Canceled', 'Failed', 'Issued')

    def __init__(self, config, connector):
        self._connector = connector
        self._config = config

    @staticmethod
    def help():
        return 'Query task missions'

    @staticmethod
    def add_arguments(parser):
        parser.add_argument('-l', '--list', dest='list',
                            help='List ready missions', action='store_true')

        parser.add_argument('-a', '--list-all', dest='list_all',
                            help='List all missions', action='store_true')

        parser.add_argument('-m', '--mid', dest='mid',
                            help='Mission ID (mid)', action='store')

        parser.add_argument('-n', '--name', dest='name',
                            help='Mission name', action='store')

        parser.add_argument('-M', '--email', dest='email',
                            help='Email address of issuer', action='store')

    def run(self):
        if self._config.list or self._config.list_all:
            print(self._connector.mission_list(self._config.list_all))
        else:
            where = {key: value
                     for key, value in vars(self._config).items()
                     if key in ['mid', 'name', 'email'] and value}

            print(self._connector.mission_info(**where))
