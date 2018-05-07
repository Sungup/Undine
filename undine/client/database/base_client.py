from undine.database.rabbitmq import RabbitMQConnector, RabbitMQRpcClient
from undine.utils.exception import UndineException, VirtualMethodException

import json


class BaseClient:
    def __init__(self, config=None):
        self._config = config

    def _has_config_entry(self, name):
        return isinstance(self._config, dict) and name in self._config

    def _get_task_queue(self):
        if not self._has_config_entry('task_queue'):
            raise UndineException('Missing RabbitMQ option field (task_queue)')

        return RabbitMQConnector(self._config['task_queue'], consumer=False)

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

    # TODO Change reset task.
    # This function will be changed into cancel and redo function.
    # reset_task will be deprecated before releases.
    def reset_task(self, **kwargs):
        # Retrieve only tid, mid, state field from inserted arguments
        kwargs = {k: v
                  for k, v in kwargs.items()
                  if k in ('tid', 'mid', 'state')}

        if not kwargs:
            raise UndineException('Any argument has not been selected.')

        queue = self._get_task_queue()
        for task in [dict(zip(('tid', 'cid', 'iid', 'wid'), item))
                     for item in self._reset_list(**kwargs)]:
            self._reset_task(task['tid'])
            queue.publish(json.dumps(task))

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

    # TODO Will be deprecated.
    def _reset_list(self, **kwargs):
        raise VirtualMethodException(BaseClient, '_reset_list (internal)')

    # TODO Will be deprecated.
    def _reset_task(self, tid):
        raise VirtualMethodException(BaseClient, '_reset_task (internal)')
