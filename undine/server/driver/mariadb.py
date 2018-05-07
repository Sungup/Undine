from undine.server.driver.base_driver import BaseNetworkDriver
from undine.server.information import ConfigInfo, WorkerInfo, InputInfo
from undine.server.information import TaskInfo
from undine.database.mariadb import MariaDbConnector


class MariaDbDriver(BaseNetworkDriver):
    _QUERY = {
        'task': '''
            SELECT HEX(tid), HEX(cid), HEX(iid), HEX(wid), reportable
              FROM task 
             WHERE tid = UNHEX(%s)
        ''',
        'config': '''
            SELECT HEX(cid), name, config FROM config
             WHERE cid = UNHEX(%s)
        ''',
        'worker': '''
            SELECT HEX(wid), worker_dir, command, arguments
              FROM worker
             WHERE wid = UNHEX(%s)
        ''',
        'input': '''
            SELECT HEX(iid), name, items
              FROM input
             WHERE iid = UNHEX(%s)
        ''',
        'state': '''
            UPDATE task 
               SET state = %(state)s, host = %(host)s, ip = INET_ATON(%(ip)s)
             WHERE tid = UNHEX(%(tid)s)
        ''',
        'result': '''
            INSERT INTO result(tid, content)
                 VALUES (UNHEX(%(tid)s), %(content)s)
        ''',
        'error': '''
            INSERT INTO error(tid, message)
                 VALUES (UNHEX(%(tid)s), %(message)s)
        ''',
        'cleanup': '''
            UPDATE task
               SET state = 'C'
             WHERE ip = INET_ATON(%(ip)s) AND state = 'I'
        ''',
        'login': '''
            INSERT INTO host(name, ip, logged_in)
                 VALUES(%(name)s, INET_ATON(%(ip)s), CURRENT_TIMESTAMP)
            ON DUPLICATE KEY
            UPDATE name = %(name)s, logged_in = CURRENT_TIMESTAMP
        ''',
        'logout': '''
            UPDATE host
               SET logged_out = CURRENT_TIMESTAMP
             WHERE ip = INET_ATON(%(ip)s)
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
        row = self._mariadb.fetch_a_tuple(self._QUERY['config'], (cid, ))

        return ConfigInfo(cid=row[0], name=row[1], config=row[2],
                          dir=self._config_dir,
                          ext=self._config_ext)

    def worker(self, wid):
        row = self._mariadb.fetch_a_tuple(self._QUERY['worker'], (wid, ))

        return WorkerInfo(wid=row[0], dir=row[1], cmd=row[2], arguments=row[3])

    def inputs(self, iid):
        row = self._mariadb.fetch_a_tuple(self._QUERY['input'], (iid, ))

        return InputInfo(iid=row[0], name=row[1], items=row[2])

    def _task(self, tid):
        row = self._mariadb.fetch_a_tuple(self._QUERY['task'], (tid, ))

        return TaskInfo(tid=row[0], cid=row[1], iid=row[2], wid=row[3],
                        reportable=row[4])

    def _preempt(self, info):
        info['state'] = 'I'

        self._mariadb.execute_single_dml(self._QUERY['state'], info)

        return True

    def _done(self, info, content, report):
        info['state'] = 'D'
        item = {'tid': info['tid'], 'content': content}

        queries = [self._mariadb.SQLItem(self._QUERY['state'], info)]

        if report:
            queries.append(self._mariadb.SQLItem(self._QUERY['result'], item))

        self._mariadb.execute_multiple_dml(queries)

        return True

    def _cancel(self, info):
        info['state'] = 'C'
        self._mariadb.execute_single_dml(self._QUERY['state'], info)

    def _fail(self, info, message):
        info['state'] = 'F'
        item = {'tid': info['tid'], 'message': message}

        queries = [self._mariadb.SQLItem(self._QUERY['state'], info),
                   self._mariadb.SQLItem(self._QUERY['error'], item)]

        self._mariadb.execute_multiple_dml(queries)

        self._error_logging('tid({0})'.format(info['tid']), message)

    def _logged_in(self):
        host = {'ip': self.host.ipv4, 'name': self.host.name}

        queries = [self._mariadb.SQLItem(self._QUERY['cleanup'], host),
                   self._mariadb.SQLItem(self._QUERY['login'], host)]

        self._mariadb.execute_multiple_dml(queries)

    def _logged_out(self):
        host = {'ip': self.host.ipv4}

        self._mariadb.execute_single_dml(self._QUERY['logout'], host)