from undine.database.rabbitmq import RabbitMQConnector
from undine.driver.driver_base import DriverBase
from undine.utils.exception import UndineException

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

    def fetch(self):
        return self._task(json.loads(self._queue.consume())['tid'])

    def _task(self, _tid):
        raise UndineException('This method is the abstract method of preempt')

    def wait_others(self):
        return True
