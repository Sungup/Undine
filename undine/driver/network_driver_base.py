from undine.driver.driver_base import DriverBase
from undine.utils.exception import UndineException

import pika
import json


class NetworkDriverBase(DriverBase):
    #
    # Constructor & Destructor
    #
    def __init__(self, rabbitmq, config, config_dir):
        DriverBase.__init__(self, config, config_dir)

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

    def __del__(self):
        self._conn.close()

    def fetch(self):
        for frame, _, body in self._channel.consume(queue=self._queue):
            self._channel.basic_ack(frame.delivery_tag)

            return self.task(json.loads(body)['tid'])

    def task(self, _tid):
        raise UndineException('This method is the abstract method of preempt')

    def wait_others(self):
        return True
