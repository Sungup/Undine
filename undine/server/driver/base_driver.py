from undine.database.rabbitmq import RabbitMQConnector
from undine.utils.exception import UndineException, VirtualMethodException
from undine.utils.system import print_console_header
from undine.utils.system import System

import undine.utils.logging as logging

import json


class BaseDriver:
    _DRIVER_LOGGER_NAME = 'undine-driver'
    _DRIVER_LOGGER_PATH = '/tmp/{}.log'.format(_DRIVER_LOGGER_NAME)
    _DRIVER_LOGGER_LEVEL = 'ERROR'

    _DEFAULT_CONFIG_EXT = '.json'

    _ERROR_LOG_START = print_console_header('Error Message Start', '=')
    _ERROR_LOG_END = print_console_header('Error Message End', '=')

    def __init__(self, config, config_dir):
        self._config_dir = config_dir
        self._config_ext = config.setdefault('config_ext',
                                             self._DEFAULT_CONFIG_EXT)

        # Create logger instance
        log_path = config.setdefault('log_file', self._DRIVER_LOGGER_PATH)
        log_level = config.setdefault('log_level', self._DRIVER_LOGGER_LEVEL)

        self._logger = logging.get_logger(self._DRIVER_LOGGER_NAME,
                                          log_path, log_level)

    def _error_logging(self, title, body):
        self._logger.error('{0}\n{2}\n{1}\n{3}\n'.format(title, body,
                                                         self._ERROR_LOG_START,
                                                         self._ERROR_LOG_END))

    def fetch(self):
        raise VirtualMethodException(self.__class__, 'fetch')

    def config(self, _cid):
        raise VirtualMethodException(self.__class__, 'config')

    def worker(self, _wid):
        raise VirtualMethodException(self.__class__, 'worker')

    def inputs(self, _iid):
        raise VirtualMethodException(self.__class__, 'input')

    def preempt(self, _tid):
        raise VirtualMethodException(self.__class__, 'preempt')

    def done(self, _tid, _contents, _report):
        raise VirtualMethodException(self.__class__, 'done')

    def cancel(self, _tid):
        raise VirtualMethodException(self.__class__, 'cancel')

    def fail(self, _tid, _message):
        raise VirtualMethodException(self.__class__, 'fail')

    def is_ready(self):
        raise VirtualMethodException(self.__class__, '_wait_others')


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

        self._logged_in()

    def __del__(self):
        self._logged_out()

    #
    # Private method
    #
    def __params(self, tid, **kwargs):
        return dict(tid=tid, host=self._host.name, ip=self._host.ipv4, **kwargs)

    @property
    def host(self):
        return self._host

    @property
    def _ip(self):
        return self._host.ipv4

    @property
    def _hostname(self):
        return self._host.name

    #
    # Protected inherited interface
    #
    def _task(self, _tid):
        raise VirtualMethodException(self.__class__, '_task')

    def _preempt(self, _tid, _host, _ip):
        raise VirtualMethodException(self.__class__, '_preempt')

    def _done(self, _tid, _host, _ip, _content, _report):
        raise VirtualMethodException(self.__class__, '_done')

    def _cancel(self, _tid, _host, _ip):
        raise VirtualMethodException(self.__class__, '_cancel')

    def _fail(self, _tid, _host, _ip, _message):
        raise VirtualMethodException(self.__class__, '_fail')

    def _logged_in(self):
        raise VirtualMethodException(self.__class__, '_logged_in')

    def _logged_out(self):
        raise VirtualMethodException(self.__class__, '_logged_out')

    #
    # Public interface
    #
    def fetch(self):
        return self._task(json.loads(self._queue.consume())['tid'])

    def preempt(self, tid):
        return self._preempt(**self.__params(tid))

    def done(self, tid, content, report):
        return self._done(**self.__params(tid, content=content, report=report))

    def cancel(self, tid):
        return self._cancel(**self.__params(tid))

    def fail(self, tid, message):
        return self._fail(**self.__params(tid, message=message))

    def is_ready(self):
        return True
