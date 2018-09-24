from collections import namedtuple

__OptItem = namedtuple('__OptItem',
                       ('name', 'type', 'default', 'metavar', 'visible',
                        'help'))


class __Options:
    def __init__(self, name, *options):
        self.__name = name
        self.__options = options

    def __optname(self, name, prefix=''):
        return '{}{}.{}'.format(prefix, self.__name, name)

    def add_argument(self, parser):
        for opt in self.__options:
            if not opt.visible:
                continue

            parser.add_argument('--{}'.format(self.__optname(opt.name)),
                                dest=self.__optname(opt.name),
                                type=opt.type, default=opt.default,
                                metavar=opt.metavar, action='store',
                                help=opt.help)

    def parse_args(self, **kwargs):
        return {
            o.name: kwargs[self.__optname(o.name)] if o.visible else o.default
            for o in self.__options
        }

    @property
    def name(self):
        return self.__name


__DEFAULT_CONFIG = {
    'mariadb-cli': [
        __Options(
            'database',
            __OptItem('type', str, 'mariadb', 'DB_TYPE', False,
                      "Database type. Don't use this option."),
            __OptItem('host', str, '<DB_HOST_ADDRESS>', 'ADDRESS', True,
                      'Your database host address'),
            __OptItem('database', str, '<DB_NAME>', 'DB_NAME', True,
                      'Database name'),
            __OptItem('user', str, '<DB_USER_ID>', 'ID', True,
                      'Database account id'),
            __OptItem('password', str, '<DB_USER_PWD>', 'PASSWORD', True,
                      'Database account password')
        ),
        __Options(
            'task_queue',
            __OptItem('host', str, '<RABBITMQ_HOST_ADDRESS>', 'ADDRESS', True,
                      'Your RabbitMQ host address for the global task queue'),
            __OptItem('vhost', str, '<RABBITMQ_VHOST_NAME>', 'NAME', True,
                      'RabbitMQ vhost name'),
            __OptItem('queue', str, '<RABBITMQ_QUEUE_NAME>', 'QUEUE', True,
                      'RabbitMQ task queue name'),
            __OptItem('user', str, '<RABBITMQ_USER_ID>', 'ID', True,
                      'RabbitMQ account id'),
            __OptItem('password', str, '<RABBITMQ_USER_PWD>', 'PASSWORD', True,
                      'RabbitMQ account password')
        ),
        __Options(
            'rpc',
            __OptItem('host', str, '<RABBITMQ_HOST_ADDRESS>', 'ADDRESS', True,
                      'Your RabbitMQ host address for the RPC server'),
            __OptItem('vhost', str, '<RABBITMQ_VHOST_NAME>', 'NAME', True,
                      'RabbitMQ vhost name for the RPC'),
            __OptItem('user', str, '<RABBITMQ_USER_ID>', 'ID', True,
                      'RabbitMQ account id for the RPC'),
            __OptItem('password', str, '<RABBITMQ_USER_PWD>', 'PASSWORD', True,
                      'RabbitMQ account password for the RPC')
        )
    ],
    'mariadb-svr': [
        __Options(
            'manager',
            __OptItem('config_dir', str, '/tmp/undine/config', 'DIR', True,
                      'Config directory for the temporary task config file.'),
            __OptItem('result_dir', str, '/tmp/undine/result', 'DIR', True,
                      'Result directory for the temporary task result file.'),
            __OptItem('result_ext', str, '.log', 'EXT', True,
                      'Result file extension'),
            __OptItem('input_dir', str, '<INPUT_FILE_HOME_PATH>', 'DIR', True,
                      "Input files' home directory")
        ),
        __Options(
            'scheduler',
            __OptItem('worker_max', int, 16, 'WORKERS', True,
                      'Number of total workers on this system.'),
            __OptItem('log_file', str, '/tmp/undine/sched.log', 'PATH', True,
                      'Scheduler log file path'),
            __OptItem('log_level', str, 'info', 'LEVEL', True,
                      'Scheduler log inform level')
        ),
        __Options(
            'driver',
            __OptItem('type', str, 'mariadb', 'DB_TYPE', False,
                      "Database type. Don't use this option."),
            __OptItem('config_ext', str, '.json', 'EXT', True,
                      'File extension of the temporary config file'),
            __OptItem('log_file', str, '/tmp/undine/driver.log', 'PATH', True,
                      'Task driver log file path'),
            __OptItem('log_level', str, 'info', 'LEVEL', True,
                      'Task driver log inform level'),
            __OptItem('host', str, '<DB_HOST_ADDRESS>', 'ADDRESS', True,
                      'Your database host address'),
            __OptItem('database', str, '<DB_NAME>', 'DB_NAME', True,
                      'Database name'),
            __OptItem('user', str, '<DB_USER_ID>', 'ID', True,
                      'Database account id'),
            __OptItem('password', str, '<DB_USER_PWD>', 'PASSWORD', True,
                      'Database account password')
        ),
        __Options(
            'task_queue',
            __OptItem('host', str, '<RABBITMQ_HOST_ADDRESS>', 'ADDRESS', True,
                      'Your RabbitMQ host address for the global task queue'),
            __OptItem('vhost', str, '<RABBITMQ_VHOST_NAME>', 'NAME', True,
                      'RabbitMQ vhost name'),
            __OptItem('queue', str, '<RABBITMQ_QUEUE_NAME>', 'QUEUE', True,
                      'RabbitMQ task queue name'),
            __OptItem('user', str, '<RABBITMQ_USER_ID>', 'ID', True,
                      'RabbitMQ account id'),
            __OptItem('password', str, '<RABBITMQ_USER_PWD>', 'PASSWORD', True,
                      'RabbitMQ account password')
        ),
        __Options(
            'rpc',
            __OptItem('host', str, '<RABBITMQ_HOST_ADDRESS>', 'ADDRESS', True,
                      'Your RabbitMQ host address for the RPC server'),
            __OptItem('vhost', str, '<RABBITMQ_VHOST_NAME>', 'NAME', True,
                      'RabbitMQ vhost name for the RPC'),
            __OptItem('user', str, '<RABBITMQ_USER_ID>', 'ID', True,
                      'RabbitMQ account id for the RPC'),
            __OptItem('password', str, '<RABBITMQ_USER_PWD>', 'PASSWORD', True,
                      'RabbitMQ account password for the RPC')
        )
    ],
    'sqlite3': [
        __Options(
            'manager',
            __OptItem('config_dir', str, '/tmp/undine/config', 'DIR', True,
                      'Config directory for the temporary task config file.'),
            __OptItem('result_dir', str, '/tmp/undine/result', 'DIR', True,
                      'Result directory for the temporary task result file.'),
            __OptItem('result_ext', str, '.log', 'EXT', True,
                      'Result file extension'),
            __OptItem('input_dir', str, '<INPUT_FILE_HOME_PATH>', 'DIR', True,
                      "Input files' home directory")
        ),
        __Options(
            'scheduler',
            __OptItem('worker_max', int, 16, 'WORKERS', True,
                      'Number of total workers on this system.'),
            __OptItem('log_file', str, '/tmp/undine/sched.log', 'PATH', True,
                      'Scheduler log file path'),
            __OptItem('log_level', str, 'info', 'LEVEL', True,
                      'Scheduler log inform level')
        ),
        __Options(
            'driver',
            __OptItem('type', str, 'sqlite', 'DB_TYPE', False,
                      "Database type. Don't use this option."),
            __OptItem('config_ext', str, '.json', 'EXT', True,
                      'File extension of the temporary config file'),
            __OptItem('log_file', str, '/tmp/undine/driver.log', 'PATH', True,
                      'Task driver log file path'),
            __OptItem('log_level', str, 'info', 'LEVEL', True,
                      'Task driver log inform level'),
            __OptItem('db_file', str, 'missions.sqlite3', 'PATH', True,
                      'SQLite3 DB file containing all configurations to run')
        )
    ]
}
