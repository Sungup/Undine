from collections import namedtuple
from mysql.connector.pooling import MySQLConnectionPool as MariaDBConnectionPool
from undine.utils.exception import UndineException

import mysql.connector as mariadb


class MariaDbConnector:
    _DEFAULT_HOST = 'localhost'
    _DEFAULT_DATABASE = 'undine'
    _DEFAULT_USER = 'undine'
    _DEFAULT_PASSWD = 'password'

    SQLItem = namedtuple('SQLItem', ['operation', 'params'])

    def __init__(self, config):
        db_config = {
            'host': config.setdefault('host', self._DEFAULT_HOST),
            'database': config.setdefault('database', self._DEFAULT_DATABASE),
            'user': config.setdefault('user', self._DEFAULT_USER),
            'passwd': config.setdefault('password', self._DEFAULT_PASSWD)
        }

        try:
            self._pool = MariaDBConnectionPool(pool_name=db_config['database'],
                                               **db_config)

        except mariadb.Error as error:
            raise UndineException('MariaDB connection failed: {}'.format(error))

    def sql_item(self, operation, params=tuple()):
        return self.SQLItem(operation, params)

    def fetch_a_tuple(self, query, params=tuple()):
        conn = self._pool.get_connection()
        cursor = conn.cursor()

        cursor.execute(query, params)
        row = cursor.fetchone()
        cursor.close()

        conn.close()

        return row

    def execute_multiple_dml(self, execute_items):
        conn = self._pool.get_connection()

        cursor = conn.cursor()

        for item in execute_items:
            cursor.execute(item.operation, item.params)

        cursor.close()

        conn.commit()
        conn.close()

    def execute_single_dml(self, query, params):
        conn = self._pool.get_connection()

        cursor = conn.cursor()
        cursor.execute(query, params)
        cursor.close()

        conn.commit()
        conn.close()
