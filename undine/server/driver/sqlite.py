from os import path
from undine.database.sqlite import SQLiteConnector
from undine.server.driver.base_driver import BaseDriver
from undine.server.information import ConfigInfo, WorkerInfo, InputInfo
from undine.server.information import TaskInfo
from undine.utils.exception import UndineException


class SQLiteDriver(BaseDriver):
    _QUERY = {
        'fetch': "SELECT tid, cid, iid, wid, reportable FROM task "
                 "WHERE state = 'R' LIMIT 1",
        'config': "SELECT cid, name, config FROM config "
                  "WHERE cid = ?",
        'worker': "SELECT wid, worker_dir, command, arguments, file_input"
                  "  FROM worker WHERE wid = ?",
        'input': "SELECT iid, name, items FROM input "
                 "WHERE iid = ?",
        'preempt': "UPDATE task SET state ='I' WHERE tid = ?",
        'done': "UPDATE task SET state ='D' WHERE tid = ?",
        'cancel': "UPDATE task SET state ='C' WHERE tid = ?",
        'fail': "UPDATE task SET state ='F' WHERE tid = ?",
        'result': "INSERT INTO result VALUES (:tid, :content)",
        'error': "INSERT INTO error VALUES (:tid, :message)",
        'count': "SELECT COUNT(tid) FROM task WHERE state ='R'"
    }

    #
    # Constructor & Destructor
    #
    def __init__(self, config, config_dir):
        BaseDriver.__init__(self, config, config_dir)

        # 1. Check input parameter is no missing
        if 'db_file' not in config:
            raise UndineException("'db_file' is not set in driver section")

        if not path.isfile(config['db_file']):
            raise UndineException("'db_file' is not exists")

        self._sqlite = SQLiteConnector(config)

    #
    # Inherited methods
    #
    def fetch(self):
        row = self._sqlite.fetch_a_tuple(self._QUERY['fetch'])

        return TaskInfo(tid=row[0], cid=row[1], iid=row[2], wid=row[3],
                        reportable=bool(row[4]))

    def config(self, cid):
        row = self._sqlite.fetch_a_tuple(self._QUERY['config'], cid)

        return ConfigInfo(cid=row[0], name=row[1], config=row[2],
                          dir=self._config_dir,
                          ext=self._config_ext)

    def worker(self, wid):
        row = self._sqlite.fetch_a_tuple(self._QUERY['worker'], wid)

        return WorkerInfo(wid=row[0], dir=row[1], cmd=row[2],
                          arguments=row[3], file_input=row[4])

    def inputs(self, iid):
        row = self._sqlite.fetch_a_tuple(self._QUERY['input'], iid)

        return InputInfo(iid=row[0], name=row[1], items=row[2])

    def preempt(self, tid):
        self._sqlite.execute_single_dml(self._QUERY['preempt'], tid)

        return True

    def done(self, tid, content, report):
        queries = [self._sqlite.sql(self._QUERY['done'], tid)]

        if report:
            queries.append(self._sqlite.sql(self._QUERY['result'],
                                            tid=tid, content=content))

        self._sqlite.execute_multiple_dml(queries)

        return True

    def cancel(self, tid):
        self._sqlite.execute_single_dml(self._QUERY['cancel'], tid)

    def fail(self, tid, message):
        queries = [self._sqlite.sql(self._QUERY['fail'], tid),
                   self._sqlite.sql(self._QUERY['error'],
                                    tid=tid, message=message)]

        self._sqlite.execute_multiple_dml(queries)

        self._error_logging('tid({0})'.format(tid), message)

    def is_ready(self):
        return bool(self._sqlite.fetch_a_tuple(self._QUERY['count'])[0])
