from undine.utils.exception import UndineException
from undine.information import ConfigInfo, WorkerInfo, InputInfo, TaskInfo

import json
import pika
import uuid


class ClientBase:
    def __init__(self, rabbitmq):
        if rabbitmq is None:
            raise UndineException('Missing RabbitMQ option field (rabbitmq)')

        credential = pika.PlainCredentials(rabbitmq['user'],
                                           rabbitmq['password'])

        parameter = pika.ConnectionParameters(host=rabbitmq['host'],
                                              virtual_host=rabbitmq['vhost'],
                                              credentials=credential)

        self._conn = pika.BlockingConnection(parameter)
        self._channel = self._conn.channel()
        self._queue = rabbitmq['queue']

        self._property = pika.BasicProperties(delivery_mode=2)

        self._channel.queue_declare(queue=self._queue, durable=True)

    @staticmethod
    def _get_uuid():
        return str(uuid.uuid4()).replace('-', '')

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
        self._channel.basic_publish(exchange='',
                                    routing_key=self._queue,
                                    body=json.dumps(task),
                                    properties=self._property)
        return task['tid']

    #
    # Protected inherite methods
    #
    def _insert_worker(self, _worker):
        raise UndineException('This method is the abstract method of fetch')

    def _insert_input(self, _input):
        raise UndineException('This method is the abstract method of fetch')

    def _insert_config(self, _config):
        raise UndineException('This method is the abstract method of fetch')

    def _insert_task(self, _task):
        raise UndineException('This method is the abstract method of fetch')
