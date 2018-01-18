from undine.utils.exception import UndineException

# For factory item
import undine.driver.file_driver
import undine.driver.json_driver
import undine.driver.sqlite_driver
import undine.driver.mariadb_driver


class TaskDriverFactory:
    _INVALID_TYPE = "Unsupported driver type '{}'"

    _STAND_ALONE = {
        'file': undine.driver.file_driver.FileDriver,
        'json': undine.driver.json_driver.JSONDriver,
        'sqlite': undine.driver.sqlite_driver.SQLiteDriver
    }

    _ONLINE = {
        'mariadb': undine.driver.mariadb_driver.MariaDbDriver
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
