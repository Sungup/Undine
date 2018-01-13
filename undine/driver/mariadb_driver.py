from collections import namedtuple
from mysql.connector.pooling import MySQLConnectionPool as MariaDBConnectionPool
from undine.driver.network_driver_base import NetworkDriverBase
from undine.utils.exception import UndineException
from undine.information import ConfigInfo, WorkerInfo, InputInfo, TaskInfo

import mysql.connector as mariadb


class MariaDbDriver(NetworkDriverBase):
    _DEFAULT_HOST = 'localhost'
    _DEFAULT_DATABASE = 'undine'
    _DEFAULT_USER = 'undine'
    _DEFAULT_PASSWD = 'password'

    _QUERY = {
        'task': "SELECT HEX(tid), HEX(cid), HEX(iid), HEX(wid) FROM task "
                "WHERE tid = UNHEX(%s)",
        'config': "SELECT HEX(cid), name, config FROM config "
                  "WHERE cid = UNHEX(%s)",
        'worker': "SELECT HEX(wid), worker_dir, command, arguments FROM worker "
                  "WHERE wid = UNHEX(%s)",
        'input': "SELECT HEX(iid), name, items FROM input "
                 "WHERE iid = UNHEX(%s)",
        'preempt': "UPDATE task SET state = 'I' WHERE tid = UNHEX(%s)",
        'done': "UPDATE task SET state = 'D' WHERE tid = UNHEX(%s)",
        'cancel': "UPDATE task SET state = 'C' WHERE tid = UNHEX(%s)",
        'fail': "UPDATE task SET state = 'F' WHERE tid = UNHEX(%s)",
        'result': "INSERT INTO result(tid, content) "
                  "VALUES (UNHEX(%(tid)s), %(content)s)",
        'error': "INSERT INTO error(tid, message) "
                 "VALUES (UNHEX(%(tid)s), %(message)s)",
    }

    #
    # Constructor & Destructor
    #
    def __init__(self, rabbitmq, config, config_dir):
        NetworkDriverBase.__init__(self, rabbitmq, config, config_dir)

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

    #
    # Private methods
    #
    ExecuteItem = namedtuple('ExecuteItem', ['operation', 'params'])

    def _select_a_tuple(self, query, params):
        conn = self._pool.get_connection()
        cursor = conn.cursor()

        cursor.execute(query, params)
        row = cursor.fetchone()

        cursor.close()
        conn.close()

        return row

    def _execute_dml(self, execute_items):
        conn = self._pool.get_connection()
        cursor = conn.cursor()

        for item in execute_items:
            cursor.execute(item.operation, item.params)

        cursor.close()

        conn.commit()
        conn.close()

    #
    # Inherited methods
    #
    def task(self, tid):
        row = self._select_a_tuple(self._QUERY['task'], (tid, ))

        return TaskInfo(tid=row[0], cid=row[1], iid=row[2], wid=row[3])

    def config(self, cid):
        row = self._select_a_tuple(self._QUERY['config'], (cid, ))

        return ConfigInfo(cid=row[0], name=row[1], config=row[2],
                          dir=self._config_dir,
                          ext=self._config_ext)

    def worker(self, wid):
        row = self._select_a_tuple(self._QUERY['worker'], (wid, ))

        return WorkerInfo(wid=row[0], dir=row[1], cmd=row[2], arguments=row[3])

    def inputs(self, iid):
        row = self._select_a_tuple(self._QUERY['input'], (iid, ))

        return InputInfo(iid=row[0], name=row[1], items=row[2])

    def preempt(self, tid):
        queries = [self.ExecuteItem(self._QUERY['preempt'], (tid,))]
        self._execute_dml(queries)

        return True

    def done(self, tid, content):
        item = {'tid': tid, 'content': content}

        queries = [self.ExecuteItem(self._QUERY['done'], (tid,)),
                   self.ExecuteItem(self._QUERY['result'], item)]

        self._execute_dml(queries)

        return True

    def cancel(self, tid):
        queries = [self.ExecuteItem(self._QUERY['cancel'], (tid,))]

        self._execute_dml(queries)

    def fail(self, tid, message):
        item = {'tid': tid, 'message': message}

        queries = [self.ExecuteItem(self._QUERY['fail'], (tid,)),
                   self.ExecuteItem(self._QUERY['error'], item)]

        self._execute_dml(queries)

        self._error_logging('tid({0})'.format(tid), message)
