from undine.database.rabbitmq import RabbitMQConnector
from undine.server.driver.base_driver import BaseDriver
from undine.utils.exception import UndineException, VirtualMethodException
from undine.utils.system import System

import json


class BaseNetworkDriver(BaseDriver):
    #
    # Constructor & Destructor
    #
    def __init__(self, task_queue, config, config_dir):
        BaseDriver.__init__(self, config, config_dir)

        if task_queue is None:
            raise UndineException('Missing RabbitMQ option field (task_queue)')

        self._queue = RabbitMQConnector(task_queue)
        self._host = System.host_info()

    #
    # Private method
    #
    def _make_params(self, tid):
        return {'tid': tid, 'host': self._host.name, 'ip': self._host.ipv4}

    #
    # Protected inherited interface
    #
    def _task(self, _tid):
        raise VirtualMethodException(self.__class__, '_task')

    def _preempt(self, _info):
        raise VirtualMethodException(self.__class__, '_preempt')

    def _done(self, _info, _content, _report):
        raise VirtualMethodException(self.__class__, '_done')

    def _cancel(self, _info):
        raise VirtualMethodException(self.__class__, '_cancel')

    def _fail(self, _info, _message):
        raise VirtualMethodException(self.__class__, '_fail')

    #
    # Public interface
    #
    def fetch(self):
        return self._task(json.loads(self._queue.consume())['tid'])

    def preempt(self, tid):
        return self._preempt(self._make_params(tid))

    def done(self, tid, content, report):
        return self._done(self._make_params(tid), content, report)

    def cancel(self, tid):
        return self._cancel(self._make_params(tid))

    def fail(self, tid, message):
        return self._fail(self._make_params(tid), message)

    def is_ready(self):
        return True
