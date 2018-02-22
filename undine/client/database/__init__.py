from undine.utils.exception import UndineException

# For factory item
from undine.client.database.mariadb import MariaDbClient

from undine.client.database.wrapper.cli import CliWrapper
from undine.client.database.wrapper.json import JsonWrapper


class ClientFactory:
    _INVALID_TYPE = "Unsupported driver type '{}'"

    _DB_DRIVER = {
        'mariadb': MariaDbClient
    }

    @staticmethod
    def create(config, json=False, cli=True):
        type_ = config.setdefault('type', 'Invalid')

        if type_ not in ClientFactory._DB_DRIVER:
            raise UndineException(ClientFactory._INVALID_TYPE.format(type_))

        connector = ClientFactory._DB_DRIVER[type_](config)

        if json:
            return JsonWrapper(connector)
        elif cli:
            return CliWrapper(connector)
        else:
            return connector
