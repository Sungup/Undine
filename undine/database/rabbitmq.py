from threading import Thread
from undine.utils.exception import UndineException
from pika.exceptions import ChannelClosed

import json
import pika
import uuid


class _RabbitMQConnector:
    _DEFAULT_HOST = 'localhost'
    _DEFAULT_VHOST = 'undine'
    _DEFAULT_USER = 'undine'
    _DEFAULT_PASSWD = 'password'

    def __init__(self, config, queue=None, durable=True,
                 exclusive=False, auto_delete=False,
                 rebuild=False):
        host = config.setdefault('host', self._DEFAULT_HOST)
        vhost = config.setdefault('vhost', self._DEFAULT_VHOST)
        user = config.setdefault('user', self._DEFAULT_USER)
        passwd = config.setdefault('password', self._DEFAULT_PASSWD)

        credential = pika.PlainCredentials(user, passwd)

        parameter = pika.ConnectionParameters(host=host, virtual_host=vhost,
                                              credentials=credential)

        self._conn = pika.BlockingConnection(parameter)
        self._channel = self._conn.channel()

        if not exclusive:
            if not queue:
                raise UndineException('Queue name is not specified.')

            self._queue = queue

            if rebuild:
                self._channel.queue_delete(queue=self._queue)

            self._channel.queue_declare(queue=self._queue, durable=durable,
                                        auto_delete=auto_delete)

        else:
            result = self._channel.queue_declare(exclusive=True)
            self._queue = result.method.queue

    def __del__(self):
        self._conn.close()

    def process_data_events(self):
        self._conn.process_data_events()

    @property
    def channel(self):
        return self._channel

    @property
    def queue(self):
        return self._queue


class RabbitMQConnector(_RabbitMQConnector):
    _DEFAULT_QUEUE = 'task'

    #
    # Constructor & Destructor
    #
    def __init__(self, config, consumer=True, rebuild=False):
        queue = config.setdefault('queue', self._DEFAULT_QUEUE)

        _RabbitMQConnector.__init__(self, config, queue=queue, rebuild=rebuild)

        if not consumer:
            self._property = pika.BasicProperties(delivery_mode=2)

    def publish(self, body):
        self.channel.basic_publish(exchange='',
                                   routing_key=self._queue,
                                   body=body,
                                   properties=self._property)

    def consume(self):
        for frame, _, body in self.channel.consume(queue=self._queue):
            self.channel.basic_ack(frame.delivery_tag)

            return body


class RabbitMQRpcServer(_RabbitMQConnector):
    def __init__(self, config, queue, callback):
        _RabbitMQConnector.__init__(self, config,
                                    queue=queue,
                                    rebuild=True,
                                    durable=False,
                                    auto_delete=True)

        self._return_to = callback

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self._callback, queue=queue)

        self._thread = Thread(target=lambda: self._channel.start_consuming())

    def __del__(self):
        # Stop consuming and wait the consuming thread has been done
        self.channel.stop_consuming()

        self._thread.join()

    def _callback(self, channel, method, properties, body):
        packet = json.loads(body)

        # TODO add exception handling
        response = json.dumps(self._return_to(packet['command'],
                                              *packet['args'],
                                              **packet['kwargs']))

        props = pika.BasicProperties(correlation_id=properties.correlation_id)

        channel.basic_publish(exchange='',
                              routing_key=properties.reply_to,
                              properties=props,
                              body=response)

        channel.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self._thread.start()

    def stop(self):
        self.channel.stop_consuming()


class RabbitMQRpcClient(_RabbitMQConnector):
    def __init__(self, config, queue):
        _RabbitMQConnector.__init__(self, config, exclusive=True)

        self._correlation_id = None
        self._response_body = None
        self._rpc = queue

        self.channel.basic_consume(self._response, queue=self.queue,
                                   no_ack=True)

    def _response(self, _ch, _method, properties, body):
        if self._correlation_id == properties.correlation_id:
            self._response_body = body

    def is_ready(self):
        ready = True

        try:
            # Try queue_declare to check the rpc queue is ready.
            self.channel.queue_declare(self._rpc, passive=True)

        except ChannelClosed:
            ready = False
            pass

        return ready

    def call(self, message, *args, **kwargs):
        packet = {
            'command': message,
            'args': args,
            'kwargs': kwargs
        }

        # TODO Add timeout feature
        self._response_body = None
        self._correlation_id = str(uuid.uuid4())

        props = pika.BasicProperties(reply_to=self.queue,
                                     correlation_id=self._correlation_id)

        self.channel.basic_publish(exchange='',
                                   routing_key=self._rpc,
                                   properties=props,
                                   body=json.dumps(packet))

        while self._response_body is None:
            self.process_data_events()

        return self._response_body
