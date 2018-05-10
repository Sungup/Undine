from mysql.connector.pooling import MySQLConnectionPool as MariaDBConnectionPool
from undine.database.base import Database
from undine.utils.exception import UndineException

import mysql.connector as mariadb


class MariaDbConnector(Database):
    _DEFAULT_HOST = 'localhost'
    _DEFAULT_DATABASE = 'undine'
    _DEFAULT_USER = 'undine'
    _DEFAULT_PASSWD = 'password'

    def __init__(self, config):
        db_config = {
            'host': config.setdefault('host', self._DEFAULT_HOST),
            'database': config.setdefault('database', self._DEFAULT_DATABASE),
            'user': config.setdefault('user', self._DEFAULT_USER),
            'passwd': config.setdefault('password', self._DEFAULT_PASSWD)
        }

        try:
            self._pool = MariaDBConnectionPool(pool_name=db_config['database'],
                                               pool_size=32,
                                               **db_config)

        except mariadb.Error as error:
            raise UndineException('MariaDB connection failed: {}'.format(error))

    def _execute_multiple_dml(self, queries):
        conn = self._pool.get_connection()

        cursor = conn.cursor()

        for item in queries:
            cursor.execute(item.query, item.params)

        cursor.close()

        conn.commit()
        conn.close()

    def _execute_single_dml(self, query, params):
        conn = self._pool.get_connection()

        cursor = conn.cursor()
        cursor.execute(query, params)
        cursor.close()

        conn.commit()
        conn.close()

    def _fetch_a_tuple(self, query, params):
        conn = self._pool.get_connection()
        cursor = conn.cursor()

        cursor.execute(query, params)
        row = cursor.fetchone()
        cursor.close()

        conn.close()

        return row

    def _fetch_all_tuples(self, query, params):
        conn = self._pool.get_connection()
        cursor = conn.cursor()

        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()

        conn.close()

        return rows
