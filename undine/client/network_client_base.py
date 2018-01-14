from undine.client.client_base import ClientBase
from undine.database.rabbitmq import RabbitMQConnector
from undine.utils.exception import UndineException

import json


class NetworkClientBase(ClientBase):
    def __init__(self, rabbitmq):
        if rabbitmq is None:
            raise UndineException('Missing RabbitMQ option field (rabbitmq)')

        self._queue = RabbitMQConnector(rabbitmq, consumer=False)

    #
    # Public methods
    #
    def publish_task(self, name, cid, iid, wid):
        task = {
            'tid': self._get_uuid(),
            'name': name,
            'cid': cid,
            'iid': iid,
            'wid': wid
        }

        # Insert into remote host
        self._insert_task(task)

        # Insert into rabbitmq task queue
        self._queue.publish(json.dumps(task))

        return task['tid']
