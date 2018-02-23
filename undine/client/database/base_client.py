from undine.database.rabbitmq import RabbitMQRpcClient
from undine.utils.exception import UndineException, VirtualMethodException


class BaseClient:
    def __init__(self, config=None):
        self._config = config

    def _db_config(self):
        if isinstance(self._config, dict):
            return self._config['database']
        else:
            return None

    def rpc_call(self, ip, command, *args, **kwargs):
        if not isinstance(self._config, dict) or 'rpc' not in self._config:
            raise UndineException('RPC configuration is not exist.')

        rpc = RabbitMQRpcClient(self._config['rpc'], 'rpc-{}'.format(ip))

        return rpc.call(command, *args, **kwargs)

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
