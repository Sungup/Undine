from undine.utils.exception import VirtualMethodException
from undine.utils.system import print_console_header

import undine.utils.logging as logging


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
