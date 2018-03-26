from undine.database.rabbitmq import RabbitMQConnector, RabbitMQRpcClient
from undine.utils.exception import UndineException, VirtualMethodException


class BaseClient:
    def __init__(self, config=None):
        self._config = config

    def _has_config_entry(self, name):
        return isinstance(self._config, dict) and name in self._config

    def _db_config(self):
        if isinstance(self._config, dict):
            return self._config['database']
        else:
            return None

    def _get_task_queue(self):
        if not self._has_config_entry('task_queue'):
            raise UndineException('Missing RabbitMQ option field (task_queue)')

        return RabbitMQConnector(self._config['task_queue'], consumer=False)

    #
    # Common public methods
    #
    def rpc_call(self, ip, command, *args, **kwargs):
        if not self._has_config_entry('rpc'):
            raise UndineException('RPC configuration is not exist.')

        rpc = RabbitMQRpcClient(self._config['rpc'], 'rpc-{}'.format(ip))

        return rpc.call(command, *args, **kwargs)

    def reset_task(self, **kwargs):
        # Retrieve only tid, mid, state field from inserted arguments
        condition = ('tid', 'mid', 'state')
        kwargs = {k: v for k, v in kwargs.items() if k in condition}

        if not kwargs:
            raise UndineException('Any argument has not been selected.')

        tasks = self._reset_list(**kwargs)

        self._reset_task(**kwargs)

        # Todo add re insert tasks into task queue.

    #
    # Abstract methods
    #
    def mission_list(self, list_all=False):
        raise VirtualMethodException(BaseClient, 'mission_list')

    def mission_info(self, **kwargs):
        raise VirtualMethodException(BaseClient, 'mission_info')

    def task_list(self, **kwargs):
        raise VirtualMethodException(BaseClient, 'task_list')

    def task_info(self, tid):
        raise VirtualMethodException(BaseClient, 'task_info')

    def config_info(self, cid):
        raise VirtualMethodException(BaseClient, 'config_info')

    def input_info(self, iid):
        raise VirtualMethodException(BaseClient, 'input_info')

    def input_list(self):
        raise VirtualMethodException(BaseClient, 'input_list')

    def worker_info(self, wid):
        raise VirtualMethodException(BaseClient, 'worker_info')

    def worker_list(self):
        raise VirtualMethodException(BaseClient, 'worker_list')

    def host_list(self):
        raise VirtualMethodException(BaseClient, 'host_list')

    def _reset_list(self, **kwargs):
        raise VirtualMethodException(BaseClient, '_reset_list (internal)')

    def _reset_task(self, **kwargs):
        raise VirtualMethodException(BaseClient, '_reset_task (internal)')
