from undine.api.database.client_base import ClientBase
from undine.database.rabbitmq import RabbitMQConnector
from undine.utils.exception import UndineException, VirtualMethodException

import json


class NetworkClientBase(ClientBase):
    def __init__(self, task_queue):
        ClientBase.__init__(self, self.__default_publish_task_with_mid)

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
