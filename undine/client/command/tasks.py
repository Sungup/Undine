
class Tasks:
    _STATE = ['all', 'ready', 'issued', 'done', 'canceled', 'failed']

    def __init__(self, config, connector):
        self._connector = connector
        self._config = config

    @staticmethod
    def help():
        return 'List-up specific tasks'

    @staticmethod
    def add_arguments(parser):
        parser.add_argument('-t', '--tid', dest='tid',
                            help='Task id (tid)', action='store')

        parser.add_argument('-n', '--name', dest='name',
                            help='Task name', action='store')

        parser.add_argument('-s', '--state', dest='state', default='all',
                            choices=Tasks._STATE,
                            help='Tasks state [default: all]', action='store')

        parser.add_argument('-H', '--host', dest='host',
                            help='Hostname', action='store')

    def run(self):
        print('hello world')
