import pika


class RabbitMQConnector:
    _DEFAULT_HOST = 'localhost'
    _DEFAULT_VHOST = 'undine'
    _DEFAULT_QUEUE = 'task'
    _DEFAULT_USER = 'undine'
    _DEFAULT_PASSWD = 'password'

    #
    # Constructor & Destructor
    #
    def __init__(self, config, consumer=True, rebuild=False):
        host = config.setdefault('host', self._DEFAULT_HOST)
        vhost = config.setdefault('vhost', self._DEFAULT_VHOST)
        user = config.setdefault('user', self._DEFAULT_USER)
        passwd = config.setdefault('password', self._DEFAULT_PASSWD)

        credential = pika.PlainCredentials(user, passwd)

        parameter = pika.ConnectionParameters(host=host, virtual_host=vhost,
                                              credentials=credential)

        self._conn = pika.BlockingConnection(parameter)
        self._channel = self._conn.channel()
        self._queue = config.setdefault('queue', self._DEFAULT_QUEUE)

        if not consumer:
            self._property = pika.BasicProperties(delivery_mode=2)

        if rebuild:
            self._channel.queue_delete(queue=self._queue)

        self._channel.queue_declare(queue=self._queue, durable=True)

    def __del__(self):
        self._conn.close()

    def publish(self, body):
        self._channel.basic_publish(exchange='',
                                    routing_key=self._queue,
                                    body=body,
                                    properties=self._property)

    def consume(self):
        for frame, _, body in self._channel.consume(queue=self._queue):
            self._channel.basic_ack(frame.delivery_tag)

            return body
