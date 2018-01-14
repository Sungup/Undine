from undine.utils.exception import UndineException

import pika


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

        self._channel.queue_declare(queue=self._queue, durable=True)

    def publish_worker(self, _worker):
        raise UndineException('This method is the abstract method of fetch')

    def publish_input(self, _input):
        raise UndineException('This method is the abstract method of fetch')

    def publish_config(self, _config):
        raise UndineException('This method is the abstract method of fetch')

    def publish_task(self, _task):
        raise UndineException('This method is the abstract method of fetch')
