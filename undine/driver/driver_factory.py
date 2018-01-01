from undine.utils.exception import UndineException

# For factory item
import undine.driver.file_driver


class TaskDriverFactory:
    _INVALID_TYPE = "Unsupported driver type '{}'"

    _DRIVERS = {
        'file': undine.driver.file_driver.FileDriver
    }

    @staticmethod
    def create(config):
        type_ = config.setdefault('type', 'Invalid')

        if type_ in TaskDriverFactory._DRIVERS:
            return TaskDriverFactory._DRIVERS[type_]
        else:
            raise UndineException(TaskDriverFactory._INVALID_TYPE.format(type_))
