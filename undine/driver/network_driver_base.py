from undine.database.rabbitmq import RabbitMQConnector
from undine.driver.driver_base import DriverBase
from undine.utils.exception import UndineException
from undine.utils.system import System

import json


class NetworkDriverBase(DriverBase):
    #
    # Constructor & Destructor
    #
    def __init__(self, rabbitmq, config, config_dir):
        DriverBase.__init__(self, config, config_dir)

        if rabbitmq is None:
            raise UndineException('Missing RabbitMQ option field (rabbitmq)')

        self._queue = RabbitMQConnector(rabbitmq)
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
        raise UndineException('This method is the abstract method of _task')

    def _preempt(self, _info):
        raise UndineException('This method is the abstract method of _preempt')

    def _done(self, _info, _content):
        raise UndineException('This method is the abstract method of _done')

    def _cancel(self, _info):
        raise UndineException('This method is the abstract method of _cancel')

    def _fail(self, _info, _message):
        raise UndineException('This method is the abstract method of _fail')

    #
    # Public interface
    #
    def fetch(self):
        return self._task(json.loads(self._queue.consume())['tid'])

    def preempt(self, tid):
        return self._preempt(self._make_params(tid))

    def done(self, tid, content):
        return self._done(self._make_params(tid), content)

    def cancel(self, tid):
        return self._cancel(self._make_params(tid))

    def fail(self, tid, message):
        return self._fail(self._make_params(tid), message)

    def wait_others(self):
        return True
