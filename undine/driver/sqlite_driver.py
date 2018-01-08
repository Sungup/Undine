from os import path
from undine.driver.driver_base import DriverBase
from undine.utils.exception import UndineException
from undine.information import ConfigInfo, WorkerInfo, InputInfo, TaskInfo

import sqlite3


class SQLiteDriver(DriverBase):
    _DEFAULT_CONFIG_EXT = '.json'
    _DEFAULT_DB_FILE = 'task-set.sqlite3'

    _QUERY = {
        'fetch': "SELECT tid, cid, iid, wid FROM task "
                 "WHERE state = 'R' LIMIT 1",
        'config': "SELECT cid, name, config FROM config "
                  "WHERE cid = ?",
        'worker': "SELECT wid, worker_dir, command, arguments FROM worker "
                  "WHERE wid = ?",
        'input': "SELECT iid, name, items FROM input "
                 "WHERE iid = ?",
        'preempt': "UPDATE task SET state ='I' WHERE tid = ?",
        'done': "UPDATE task SET state ='D' WHERE tid = ?",
        'cancel': "UPDATE task SET state ='C' WHERE tid = ?",
        'fail': "UPDATE task SET state ='F' WHERE tid = ?",
        'result': "INSERT INTO result VALUES (?, ?)",
        'message': "INSERT INTO error VALUES (?, ?)",
        'count': "SELECT COUNT(tid) FROM task WHERE state ='R'"
    }

    #
    # Constructor & Destructor
    #
    def __init__(self, config, config_dir):
        DriverBase.__init__(self, config, config_dir)

        # 1. Check input parameter is no missing
        if 'db_file' not in config:
            raise UndineException("'db_file' is not set in driver section")

        # 2. Get configure values
        self._config_ext = config.setdefault('config_ext',
                                             self._DEFAULT_CONFIG_EXT)

        self._db_file = config.setdefault('db_file', self._DEFAULT_DB_FILE)

        # 3. Open db file
        if not path.isfile(self._db_file):
            raise UndineException("'db_file' is not exists")

        self._conn = sqlite3.connect(self._db_file, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._cursor = self._conn.cursor()

    def __del__(self):
        self._conn.close()

    #
    # Inherited methods
    #
    def fetch(self):
        row = self._cursor.execute(self._QUERY['fetch']).fetchone()

        return TaskInfo(tid=row[0], cid=row[1], iid=row[2], wid=row[3])

    def config(self, cid):
        row = self._cursor.execute(self._QUERY['config'], (cid, )).fetchone()

        return ConfigInfo(cid=row[0], name=row[1], config=row[2],
                          dir=self._config_dir,
                          ext=self._config_ext)

    def worker(self, wid):
        row = self._cursor.execute(self._QUERY['worker'], (wid, )).fetchone()

        return WorkerInfo(wid=row[0], dir=row[1], cmd=row[2], arguments=row[3])

    def inputs(self, iid):
        row = self._cursor.execute(self._QUERY['input'], (iid, )).fetchone()

        return InputInfo(iid=row[0], name=row[1], items=row[2])

    def preempt(self, tid):
        self._conn.execute(self._QUERY['preempt'], (tid, ))
        self._conn.commit()

        return True

    def done(self, tid, contents):
        self._conn.execute(self._QUERY['done'], (tid, ))
        self._conn.execute(self._QUERY['result'], (tid, contents))
        self._conn.commit()

        return True

    def cancel(self, tid):
        self._conn.execute(self._QUERY['cancel'], (tid, ))
        self._conn.commit()

    def fail(self, tid, message):
        self._conn.execute(self._QUERY['fail'], (tid, ))
        self._conn.execute(self._QUERY['message'], (tid, message))
        self._conn.commit()

        self._error_logging('tid({0})'.format(tid), message)

    def wait_others(self):
        return bool(self._cursor.execute(self._QUERY['count']).fetchone()[0])
