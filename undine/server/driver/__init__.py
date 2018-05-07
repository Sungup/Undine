from undine.utils.exception import UndineException

# For factory item
from undine.server.driver.file import FileDriver
from undine.server.driver.json import JSONDriver
from undine.server.driver.mariadb import MariaDbDriver
from undine.server.driver.sqlite import SQLiteDriver


class TaskDriverFactory:
    _INVALID_TYPE = "Unsupported driver type '{}'"

    _STAND_ALONE = {
        'file': FileDriver,
        'json': JSONDriver,
        'sqlite': SQLiteDriver
    }

    _ONLINE = {
        'mariadb': MariaDbDriver
    }

    @staticmethod
    def create(config, config_dir, task_queue=None):
        type_ = config.setdefault('type', 'Invalid')

        if type_ in TaskDriverFactory._STAND_ALONE:
            return TaskDriverFactory._STAND_ALONE[type_](config, config_dir)
        elif type_ in TaskDriverFactory._ONLINE:
            return TaskDriverFactory._ONLINE[type_](task_queue,
                                                    config, config_dir)
        else:
            raise UndineException(TaskDriverFactory._INVALID_TYPE.format(type_))
