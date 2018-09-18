from undine.database.rabbitmq import RabbitMQRpcServer
from undine.utils.system import System


class RpcDaemon:
    def __init__(self, config):
        queue_name = 'rpc-{}'.format(System.host_info().ipv4)

        self.__rpc = RabbitMQRpcServer(config, queue_name, self.__call)

        self.__callbacks = dict()

    @staticmethod
    def __default():
        return 'Command is not ready'

    def __call(self, command, *args, **kwargs):
        if command == 'all':
            return {name: func() for name, func in self.__callbacks.items()}
        elif command == 'list':
            return {command: ['all'] + list(self.__callbacks.keys())}
        elif command in self.__callbacks:
            return {command: self.__callbacks[command](*args, **kwargs)}
        else:
            return {command: self.__default()}

    def start(self):
        self.__rpc.start()

    def stop(self):
        self.__rpc.stop()

    def register(self, command, callback):
        self.__callbacks[command] = callback
