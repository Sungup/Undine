import abc
import re
import json

from undine.client.core import Connector
from undine.database.rabbitmq import RabbitMQRpcClient, RabbitMQConnector
from undine.utils.exception import UndineException


class BaseConnector(Connector):
    UUID4HEX = re.compile('[0-9a-f]{32}\Z', re.I)

    def __init__(self, config=None):
        self._config = config

    @property
    def _db_config(self):
        if isinstance(self._config, dict):
            return self._config['database']
        else:
            return None

    def _has_config_entry(self, name):
        return isinstance(self._config, dict) and name in self._config

    def _get_task_queue(self):
        if not self._has_config_entry('task_queue'):
            raise UndineException('Missing RabbitMQ option field (task_queue)')

        return RabbitMQConnector(self._config['task_queue'], consumer=False)

    @staticmethod
    def __check_uuid(value):
        if not BaseConnector.UUID4HEX.match(value):
            raise UndineException("ID value must fit to uuid length (32).")

        return value

    def __check_ids(self, values):
        if 'name' in values:
            queried_mid = self._get_mid(values['name'])

            # If there are mid exists.
            if queried_mid:
                values.update({'mid': queried_mid})

            del values['name']

        for id_key in ('mid', 'tid', 'cid', 'iid', 'wid'):
            if id_key in values:
                self.__check_uuid(values[id_key])

        return values

    def __get_mid(self, values):
        return self.__check_ids({k: v
                                 for k, v in values.items()
                                 if k in ('mid', 'name')})['mid']

    def __get_tid_list(self, mid, *tasks, state=None):
        if not mid and not tasks:
            raise UndineException('MID, valid mission full name, and list of '
                                  'TID can be available only')

        kwargs = {'mid': mid} if not state else {'mid': mid, 'state': state}

        return tasks if tasks else self._get_tid_list(**kwargs)

    @staticmethod
    def __manipulate_tasks(operation, tasks):
        if tasks:
            operation(*tasks)

        return len(tasks)

    #
    # Implemented public methods
    #
    def rpc_call(self, ip, command, *args, **kwargs):
        if not self._has_config_entry('rpc'):
            raise UndineException('RPC configuration is not exist.')

        rpc = RabbitMQRpcClient(self._config['rpc'], 'rpc-{}'.format(ip))

        return rpc.call(command, *args, **kwargs)

    def mission_info(self, **kwargs):
        kwargs = self.__check_ids(kwargs)

        if not kwargs:
            raise UndineException('Only MID and valid mission full name '
                                  'can be available.')

        return self._mission_info(**self.__check_ids(kwargs))

    def task_list(self, **kwargs):
        kwargs = self.__check_ids(kwargs)

        if 'mid' not in kwargs:
            raise UndineException('Only MID and valid mission full name '
                                  'can be available.')

        return self._task_list(**kwargs)

    def task_info(self, tid):
        return self._task_info(self.__check_uuid(tid))

    def config_info(self, cid):
        return self._config_info(self.__check_uuid(cid))

    def input_info(self, iid):
        return self._input_info(self.__check_uuid(iid))

    def worker_info(self, wid):
        return self._worker_info(self.__check_uuid(wid))

    def cancel_tasks(self, *tasks, **kwargs):
        # Ready => Canceled.
        # Cancel task list if task list (tasks) is not empty. Else, cancel tasks
        # which has mid from arguments (kwargs) and state is 'R' (ready)
        tasks = self.__get_tid_list(self.__get_mid(kwargs) if kwargs else None,
                                    *tasks, state='R')

        return self.__manipulate_tasks(self._cancel_tasks, tasks)

    def drop_tasks(self, *tasks, **kwargs):
        # Anything => remove.
        # Drop all tasks on given condition. But it doesn't drop the target
        # mission even if nothing tasks are in that.
        tasks = self.__get_tid_list(self.__get_mid(kwargs) if kwargs else None,
                                    *tasks)

        return self.__manipulate_tasks(self._drop_tasks, tasks)

    def drop_mission(self, **kwargs):
        # Find mid from input arguments
        mid = self.__get_mid(kwargs)

        if not mid:
            raise UndineException('Mission name and ID are only available.')

        # Call __drop_tasks not public drop_tasks. Because passed mid will be
        # recheck in public drop_tasks.
        dropped_count = self.__manipulate_tasks(self._drop_tasks,
                                                self.__get_tid_list(mid))

        self._drop_mission(mid)

        return dropped_count

    def rerun_tasks(self, *tasks, **kwargs):
        # Not ready and issued => ready.
        # Rerun command will change state to 'R' (ready) and publish task
        # message into message queue.
        tasks = self.__get_tid_list(self.__get_mid(kwargs) if kwargs else None,
                                    *tasks)

        rerun_count = self.__manipulate_tasks(self._rerun_tasks, tasks)

        queue = self._get_task_queue()
        for tid in tasks:
            queue.publish(json.dumps({'tid': tid}))

        return rerun_count

    #
    # Abstract methods.
    #
    @abc.abstractmethod
    def _get_mid(self, name): pass

    @abc.abstractmethod
    def _get_tid_list(self, **kwargs): pass

    @abc.abstractmethod
    def _mission_info(self, **kwargs): pass

    @abc.abstractmethod
    def _task_list(self, **kwargs): pass

    @abc.abstractmethod
    def _task_info(self, tid): pass

    @abc.abstractmethod
    def _config_info(self, cid): pass

    @abc.abstractmethod
    def _input_info(self, iid): pass

    @abc.abstractmethod
    def _worker_info(self, wid): pass

    @abc.abstractmethod
    def _cancel_tasks(self, *tasks): pass

    @abc.abstractmethod
    def _drop_tasks(self, *tasks): pass

    @abc.abstractmethod
    def _drop_mission(self, mid): pass

    @abc.abstractmethod
    def _rerun_tasks(self, *tasks): pass

    @abc.abstractmethod
    def mission_list(self, list_all=False): pass

    @abc.abstractmethod
    def input_list(self): pass

    @abc.abstractmethod
    def worker_list(self): pass

    @abc.abstractmethod
    def host_list(self): pass
