from undine.utils.exception import UndineException

# For factory item
from undine.client.connector.mariadb import MariaDbConnector


class ConnectorFactory:
    _INVALID_TYPE = "Unsupported connector type '{}'"

    _DB_DRIVER = {
        'mariadb': MariaDbConnector
    }

    @staticmethod
    def create(config):
        if 'database' not in config:
            raise UndineException('There is no database connection information')

        type_ = config['database'].setdefault('type', 'Invalid')

        if type_ not in ConnectorFactory._DB_DRIVER:
            raise UndineException(ConnectorFactory._INVALID_TYPE.format(type_))

        return ConnectorFactory._DB_DRIVER[type_](config)
