import logging

_LOGGING_FORMAT = '[%(asctime)s] %(levelname)s: %(message)s'
_LOGGING_DATE = '%Y-%m-%d %H:%M:%S'

_LOGGING_LEVEL = {
    'CRITICAL': logging.CRITICAL,
    'ERROR': logging.ERROR,
    'WARNING': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
    'NOTSET': logging.NOTSET
}


def get_logger(name, path, level):
    handler_ = logging.FileHandler(path)
    handler_.setFormatter(logging.Formatter(_LOGGING_FORMAT, _LOGGING_DATE))

    logger_ = logging.getLogger(name)
    logger_.addHandler(handler_)
    logger_.setLevel(_LOGGING_LEVEL[level.strip().upper()])

    return logger_


def is_debug(logger):
    return logger.level < logging.INFO
