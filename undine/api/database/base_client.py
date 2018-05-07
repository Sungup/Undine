from undine.database.rabbitmq import RabbitMQConnector
from undine.utils.exception import UndineException, VirtualMethodException

import json
import uuid


class BaseClient:
    def __init__(self, publish_task=None):
        if publish_task:
            self.publish_task = publish_task
        else:
            self.publish_task = self.__default_publish_task

    #
    # Private methods
    #
    def __default_publish_task(self, name, cid, iid, wid, report):
        task = {
            'tid': self._get_uuid(),
            'name': name,
            'cid': cid,
            'iid': iid,
            'wid': wid,
            'reportable': bool(report)
        }

        # Insert into remote host
        self._insert_task(task)

        return task['tid']

    #
    # Public methods
    #
    def publish_worker(self, name, command, arguments, worker_dir):
        worker = {
            'wid': self._get_uuid(),
            'name': name,
            'command': command,
            'arguments': arguments,
            'worker_dir': worker_dir
        }

        self._insert_worker(worker)

        return worker['wid']

    def publish_input(self, name, items):
        inputs = {
            'iid': self._get_uuid(),
            'name': name,
            'items': items if isinstance(items, str) else ','.join(items)
        }

        self._insert_input(inputs)

        return inputs['iid']

    def publish_config(self, name, content):
        config = {
            'cid': self._get_uuid(),
            'name': name,
            'config': json.dumps(content)
        }

        self._insert_config(config)

        return config['cid']

    #
    # Protected inherited methods
    #
    @staticmethod
    def _get_uuid():
        return str(uuid.uuid4()).replace('-', '')

    def _insert_worker(self, _worker):
        raise VirtualMethodException(self.__class__, '_insert_worker')

    def _insert_input(self, _inputs):
        raise VirtualMethodException(self.__class__, '_insert_input')

    def _insert_config(self, _config):
        raise VirtualMethodException(self.__class__, '_insert_config')

    def _insert_task(self, _task):
        raise VirtualMethodException(self.__class__, '_insert_task')


class BaseNetworkClient(BaseClient):
    def __init__(self, task_queue):
        BaseClient.__init__(self, self.__default_publish_task_with_mid)

        if task_queue is None:
            raise UndineException('Missing RabbitMQ option field (task_queue)')

        self._queue = RabbitMQConnector(task_queue, consumer=False)

    #
    # Private methods
    #
    def __default_publish_task_with_mid(self, name, cid, iid, wid, mid, report):
        task = {
            'tid': self._get_uuid(),
            'name': name,
            'cid': cid,
            'iid': iid,
            'wid': wid,
            'mid': mid,
            'reportable': report
        }

        # Insert into remote host
        self._insert_task(task)

        # Insert useful task information into rabbitmq task queue
        del task['name'], task['mid'], task['reportable']

        self._queue.publish(json.dumps(task))

        return task['tid']

    #
    # Protected inherited method
    #
    def _insert_mission(self, _mission):
        raise VirtualMethodException(self.__class__, '_insert_mission')

    #
    # Public methods
    #
    def publish_mission(self, name, email, description):
        mission = {
            'mid': self._get_uuid(),
            'name': name,
            'email': email,
            'description': description
        }

        self._insert_mission(mission)

        return mission['mid']
