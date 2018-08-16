from threading import Lock
from undine.database import Database

import sqlite3


class SQLiteConnector(Database):
    _DEFAULT_DB_FILE = 'task-set.sqlite3'

    def __init__(self, config):
        self._conn = sqlite3.connect(config.setdefault('db_file',
                                                       self._DEFAULT_DB_FILE),
                                     check_same_thread=False)

        self._conn.row_factory = sqlite3.Row

        self._cursor = self._conn.cursor()

        self._lock = Lock()

    def __del__(self):
        self._lock.acquire()
        self._conn.close()
        self._lock.release()

    def _execute_multiple_dml(self, queries):
        self._lock.acquire()

        for item in queries:
            self._conn.execute(item.query, item.params)

        self._conn.commit()

        self._lock.release()

    def _execute_single_dml(self, query, params):
        self._lock.acquire()

        self._conn.execute(query, params)
        self._conn.commit()

        self._lock.release()

    def _fetch_a_tuple(self, query, params):
        self._lock.acquire()

        row = self._cursor.execute(query, params).fetchone()

        self._lock.release()

        return row

    def _fetch_all_tuples(self, query, params):
        self._lock.acquire()

        rows = self._cursor.execute(query, params).fetchall()

        self._lock.release()

        return rows
