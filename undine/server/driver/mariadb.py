from undine.server.driver.base_driver import BaseNetworkDriver
from undine.server.information import ConfigInfo, WorkerInfo, InputInfo
from undine.server.information import TaskInfo
from undine.database.mariadb import MariaDbConnector


class MariaDbDriver(BaseNetworkDriver):
    _QUERY = {
        'task': '''
            SELECT tid, cid, iid, wid, reportable
              FROM task 
             WHERE tid = %s AND state = 'R'
        ''',
        'config': '''
            SELECT cid, name, config FROM config
             WHERE cid = %s
        ''',
        'worker': '''
            SELECT wid, worker_dir, command, arguments, file_input
              FROM worker
             WHERE wid = %s
        ''',
        'input': '''
            SELECT iid, name, items
              FROM input
             WHERE iid = %s
        ''',
        'state': '''
            UPDATE task 
               SET state = %(state)s, host = %(host)s, ip = %(ip)s
             WHERE tid = %(tid)s
        ''',
        'result': '''
            INSERT INTO result(tid, content)
                 VALUES (%(tid)s, %(content)s)
        ''',
        'error': '''
            INSERT INTO error(tid, message)
                 VALUES (%(tid)s, %(message)s)
        ''',
        'cleanup': '''
            UPDATE task
               SET state = 'C'
             WHERE ip = %(ip)s AND state = 'I'
        ''',
        'login': '''
            INSERT INTO host(name, ip, logged_in)
                 VALUES(%(name)s, %(ip)s, CURRENT_TIMESTAMP)
            ON DUPLICATE KEY
            UPDATE name = %(name)s, logged_in = CURRENT_TIMESTAMP
        ''',
        'logout': '''
            UPDATE host
               SET logged_out = CURRENT_TIMESTAMP
             WHERE ip = %(ip)s
        '''
    }

    #
    # Constructor & Destructor
    #
    def __init__(self, task_queue, config, config_dir):
        self._mariadb = MariaDbConnector(config)

        BaseNetworkDriver.__init__(self, task_queue, config, config_dir)

    #
    # Inherited methods
    #
    def config(self, cid):
        row = self._mariadb.fetch_a_tuple(self._QUERY['config'], cid)

        return ConfigInfo(cid=row[0], name=row[1], config=row[2],
                          dir=self._config_dir,
                          ext=self._config_ext)

    def worker(self, wid):
        row = self._mariadb.fetch_a_tuple(self._QUERY['worker'], wid)

        return WorkerInfo(wid=row[0], dir=row[1], cmd=row[2],
                          arguments=row[3], file_input=row[4])

    def inputs(self, iid):
        row = self._mariadb.fetch_a_tuple(self._QUERY['input'], iid)

        return InputInfo(iid=row[0], name=row[1], items=row[2])

    def _task(self, tid):
        row = self._mariadb.fetch_a_tuple(self._QUERY['task'], tid)

        # Only return 'Ready' state task. Other case return None
        return TaskInfo(tid=row[0], cid=row[1], iid=row[2], wid=row[3],
                        reportable=row[4]) if row else None

    def _preempt(self, tid, host, ip):
        self._mariadb.execute_single_dml(self._QUERY['state'],
                                         tid=tid, host=host, ip=ip, state='I')

        return True

    def _done(self, tid, host, ip, content, report):
        queries = [self._mariadb.sql(self._QUERY['state'],
                                     tid=tid, host=host, ip=ip, state='D')]

        if report:
            queries.append(self._mariadb.sql(self._QUERY['result'],
                                             tid=tid, content=content))

        self._mariadb.execute_multiple_dml(queries)

        return True

    def _cancel(self, tid, host, ip):
        self._mariadb.execute_single_dml(self._QUERY['state'],
                                         tid=tid, host=host, ip=ip, state='C')

    def _fail(self, tid, host, ip, message):
        queries = [self._mariadb.sql(self._QUERY['state'],
                                     tid=tid, host=host, ip=ip, state='F'),
                   self._mariadb.sql(self._QUERY['error'],
                                     tid=tid, message=message)]

        self._mariadb.execute_multiple_dml(queries)

        self._error_logging('tid({0})'.format(tid), message)

    def _logged_in(self):
        queries = [self._mariadb.sql(self._QUERY['cleanup'], ip=self._ip),
                   self._mariadb.sql(self._QUERY['login'],
                                     ip=self._ip, name=self._hostname)]

        self._mariadb.execute_multiple_dml(queries)

    def _logged_out(self):
        self._mariadb.execute_single_dml(self._QUERY['logout'], ip=self._ip)
