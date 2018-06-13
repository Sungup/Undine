from undine.database.rabbitmq import RabbitMQConnector, RabbitMQRpcClient
from undine.utils.exception import UndineException, VirtualMethodException

import inspect


class BaseClient:
    def __init__(self, config=None):
        self._config = config

    @staticmethod
    def __check_caller_type():
        stack = inspect.stack()
        if not isinstance(stack[2][0].f_locals["self"], BaseClient):
            raise UndineException("Only children of BaseClient can call "
                                  "inner_tid_list!")

    def _has_config_entry(self, name):
        return isinstance(self._config, dict) and name in self._config

    def _get_task_queue(self):
        if not self._has_config_entry('task_queue'):
            raise UndineException('Missing RabbitMQ option field (task_queue)')

        return RabbitMQConnector(self._config['task_queue'], consumer=False)

    def inner_tid_list(self, **kwargs):
        self.__check_caller_type()
        return self._tid_list(**kwargs)

    def inner_cancel_task(self, *args):
        self.__check_caller_type()
        return self._cancel_task(*args)

    #
    # Common public methods
    #
    @property
    def db_config(self):
        if isinstance(self._config, dict):
            return self._config['database']
        else:
            return None

    def rpc_call(self, ip, command, *args, **kwargs):
        if not self._has_config_entry('rpc'):
            raise UndineException('RPC configuration is not exist.')

        rpc = RabbitMQRpcClient(self._config['rpc'], 'rpc-{}'.format(ip))

        return rpc.call(command, *args, **kwargs)

    def cancel_tasks(self, **kwargs):
        # Issued => Canceled. Use only tid and mid.
        kwargs = {k: v for k, v in kwargs.items() if k in ('tid', 'mid')}

        if not kwargs:
            raise UndineException('Only TID and MID can be used.')

        # Retrieve only Issued transactions
        kwargs.update({'state': 'R'})

        tid_list = [item[0] for item in self._tid_list(**kwargs)]

        self._cancel_task(*tid_list)

        return len(tid_list)

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

    def _tid_list(self, **kwargs):
        raise VirtualMethodException(BaseClient, '_tid_list (internal)')

    def _cancel_task(self, *args):
        raise VirtualMethodException(BaseClient, '_cancel_task (internal)')
