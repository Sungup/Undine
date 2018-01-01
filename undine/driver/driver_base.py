from undine.utils.exception import UndineException
from undine.utils.system import print_console_header

import undine.utils.logging as logging


class DriverBase:
    _DRIVER_LOGGER_NAME = 'undine-driver'
    _DRIVER_LOGGER_PATH = '/tmp/{}.log'.format(_DRIVER_LOGGER_NAME)
    _DRIVER_LOGGER_LEVEL = 'ERROR'

    _ERROR_LOG_START = print_console_header('Error Message Start', '=')
    _ERROR_LOG_END = print_console_header('Error Message End', '=')

    def __init__(self, config, config_dir):
        self._config_dir = config_dir

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
        raise UndineException('This method is the abstract method of fetch')

    def config(self, _cid):
        raise UndineException('This method is the abstract method of config')

    def worker(self, _wid):
        raise UndineException('This method is the abstract method of worker')

    def inputs(self, _iid):
        raise UndineException('This method is the abstract method of inputs')

    def preempt(self, _tid):
        raise UndineException('This method is the abstract method of preempt')

    def done(self, _tid, _contents):
        raise UndineException('This method is the abstract method of task done')

    def cancel(self, _tid):
        raise UndineException('This method is the abstract method of cancel')

    def fail(self, _tid, _message):
        raise UndineException('This method is the abstract method of fail')

    def wait_others(self):
        raise UndineException('This method is the abstract method for '
                              'instance termination')
