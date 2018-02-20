from undine.database.rabbitmq import RabbitMQRpcServer
from undine.utils.system import System


class RpcDaemon:
    def __init__(self, config):
        queue_name = 'rpc-{}'.format(System.host_info().ipv4)

        self._rpc = RabbitMQRpcServer(config, queue_name, self._call)

        self._callbacks = dict()

    @staticmethod
    def _default():
        return 'Command is not ready'

    def _call(self, command, *args, **kwargs):
        if command == 'all':
            return {name: func() for name, func in self._callbacks.items()}
        elif command in self._callbacks:
            return {command: self._callbacks[command](*args, **kwargs)}
        else:
            return {command: self._default()}

    def start(self):
        self._rpc.start()

    def stop(self):
        self._rpc.stop()

    def register(self, command, callback):
        self._callbacks[command] = callback
