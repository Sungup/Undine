from collections import namedtuple
from undine.client.database.base_client import BaseClient
from undine.database.mariadb import MariaDbConnector
from undine.utils.exception import UndineException


class MariaDbClient(BaseClient):
    _QUERY = {
        'mission_list': '''
          SELECT mid, name, email,
                 ready, issued, done, canceled, failed, 
                 DATE_FORMAT(issued_at, '%Y-%m-%d %T') AS issued
            FROM mission_dashboard {where}
        ORDER BY complete, issued_at DESC
        ''',
        'mission_info': '''
          SELECT HEX(mid) AS mid, name, email, description,
                 DATE_FORMAT(issued, '%Y-%m-%d %T')
            FROM mission {where}
        ORDER BY issued ASC
        ''',
        'task_list': '''
          SELECT tid, name, host, ip, state,
                 DATE_FORMAT(issued, '%Y-%m-%d %T') AS issued,
                 DATE_FORMAT(updated, '%Y-%m-%d %T') AS updated,
                 IF(reportable = TRUE, 'true', 'false') AS reportable
            FROM task_list {where}
        ORDER BY issued ASC
        ''',
        'task_info': '''
          SELECT HEX(t.tid) AS tid, t.name AS name,
                 t.host, INET_NTOA(t.ip) AS ip, s.name AS state,
                 HEX(t.mid) AS mid,
                 HEX(t.cid) AS cid, HEX(t.iid) AS iid, HEX(t.wid) AS wid,
                 DATE_FORMAT(t.issued, '%Y-%m-%d %T') AS issued,
                 DATE_FORMAT(t.updated, '%Y-%m-%d %T') AS updated,
                 IF(t.reportable = TRUE, 'true', 'false') AS reportable,
                 IF(r.content IS NOT NULL, r.content, '-') AS result,
                 IF(r.reported IS NOT NULL,
                    DATE_FORMAT(r.reported, '%Y-%m-%d %T'), '-') AS succeed,
                 IF(e.message IS NOT NULL, e.message, '-') AS error,
                 IF(e.informed IS NOT NULL,
                    DATE_FORMAT(e.informed, '%Y-%m-%d %T'), '-') AS failed 
            FROM task AS t
            JOIN state_type AS s ON t.state = s.state
            LEFT JOIN result r ON t.tid = r.tid
            LEFT JOIN error e ON t.tid = e.tid
           WHERE t.tid = UNHEX(%(tid)s)
        ''',
        'config_info': '''
          SELECT HEX(cid) AS cid, name, config,
                 DATE_FORMAT(issued, '%Y-%m-%d %T') AS issued
            FROM config
           WHERE cid = UNHEX(%(cid)s)
        ''',
        'input_info': '''
          SELECT HEX(iid) AS iid, name, items,
                 DATE_FORMAT(issued, '%Y-%m-%d %T') AS issued
            FROM input {where}
        ORDER BY issued
        ''',
        'worker_info': '''
          SELECT HEX(wid) AS wid, name, command, arguments, worker_dir,
                 DATE_FORMAT(issued, '%Y-%m-%d %T') AS issued
            FROM worker {where}
        ORDER BY issued
        ''',
        'host_list': '''
          SELECT name, ip, issued, canceled, failed,
                 registered, logged_in, logged_out, state
            FROM host_list
        ''',
        'reset_list': '''
          SELECT HEX(tid), HEX(cid), HEX(iid), HEX(wid) 
            FROM task 
           WHERE tid in (SELECT tid FROM task {where})
        ''',
        'trash_result': '''
          INSERT INTO trash (tid, generated, category, content)
          SELECT tid, reported, 'result', content FROM result
           WHERE tid = UNHEX(%(tid)s)
        ''',
        'trash_error': '''
          INSERT INTO trash (tid, generated, category, content)
          SELECT tid, informed, 'error', message FROM error
           WHERE tid = UNHEX(%(tid)s)
        ''',
        'delete_result': '''
          DELETE FROM result WHERE tid = UNHEX(%(tid)s)
        ''',
        'delete_error': '''
          DELETE FROM error WHERE tid = UNHEX(%(tid)s)
        ''',
        'reset_task': '''
          UPDATE task
             SET state = 'R', host = NULL, ip = NULL
           WHERE tid = UNHEX(%(tid)s)
        '''
    }

    _WhereItem = namedtuple('_WhereItem', ['clause', 'format'])

    _WHERE_CLAUSE = {
        'mid': _WhereItem('mid = UNHEX(%(mid)s)', '{}'),
        'cid': _WhereItem('cid = UNHEX(%(cid)s)', '{}'),
        'iid': _WhereItem('iid = UNHEX(%(iid)s)', '{}'),
        'wid': _WhereItem('wid = UNHEX(%(wid)s)', '{}'),
        'name': _WhereItem('name LIKE %(name)s', '%{}%'),
        'host': _WhereItem('host LIKE %(host)s', '%{}%'),
        'email': _WhereItem('email LIKE %(email)s', '%{}%'),
        'state': _WhereItem('state = %(state)s', '{}'),
        'reportable': _WhereItem('reportable = %(reportable)s', None)
    }

    #
    # Constructor & Destructor
    #
    def __init__(self, config):
        BaseClient.__init__(self, config)
        self._db = MariaDbConnector(self.db_config)

    #
    # Private methods
    #
    @staticmethod
    def _value(form, value):
        return value if not form else form.format(value)

    @staticmethod
    def _where(condition):
        return 'WHERE ' + ' AND '.join(condition) if condition else ''

    def _build_query(self, query, **kwargs):
        condition = list()
        params = dict()

        for k, v in kwargs.items():
            if k in self._WHERE_CLAUSE:
                condition.append(self._WHERE_CLAUSE[k].clause)
                params[k] = self._value(self._WHERE_CLAUSE[k].format, v)

        return {'query': query.format(self._where(condition)), 'params': params}

    def _build_sql_item(self, template, **kwargs):
        return self._db.sql_item(**self._build_query(template, **kwargs))

    #
    # Inherited methods
    #
    def mission_list(self, list_all=False):
        where = 'WHERE complete = FALSE' if not list_all else ''
        query = self._QUERY['mission_list'].format(where=where)

        return self._db.fetch_all_tuples(query)

    def mission_info(self, **kwargs):
        if 'mid' in kwargs and len(kwargs['mid']) != 32:
            raise UndineException('MID value must feat to uuid length.')

        query_set = self._build_query(self._QUERY['mission_info'], **kwargs)
        return self._db.fetch_all_tuples(**query_set)

    def task_list(self, **kwargs):
        if 'mid' not in kwargs or len(kwargs['mid']) != 32:
            raise UndineException('MID value must feat to uuid length.')

        query_set = self._build_query(self._QUERY['task_list'], **kwargs)
        return self._db.fetch_all_tuples(**query_set)

    def task_info(self, tid):
        if len(tid) != 32:
            raise UndineException('TID value must feat to uuid length.')

        return self._db.fetch_a_tuple(self._QUERY['task_info'], {'tid': tid})

    def config_info(self, cid):
        if len(cid) != 32:
            raise UndineException('CID value must feat to uuid length.')

        return self._db.fetch_a_tuple(self._QUERY['config_info'], {'cid': cid})

    def input_info(self, iid):
        if len(iid) != 32:
            raise UndineException('IID value must feat to uuid length.')

        query_set = self._build_query(self._QUERY['input_info'], iid=iid)
        return self._db.fetch_a_tuple(**query_set)

    def input_list(self):
        query_set = self._build_query(self._QUERY['input_info'])

        return self._db.fetch_all_tuples(**query_set)

    def worker_info(self, wid):
        if len(wid) != 32:
            raise UndineException('WID value must feat to uuid length.')

        query_set = self._build_query(self._QUERY['worker_info'], wid=wid)
        return self._db.fetch_a_tuple(**query_set)

    def worker_list(self):
        query_set = self._build_query(self._QUERY['worker_info'])
        return self._db.fetch_all_tuples(**query_set)

    def host_list(self):
        query_set = self._build_query(self._QUERY['host_list'])
        return self._db.fetch_all_tuples(**query_set)

    # TODO will be deprecated
    def _reset_list(self, **kwargs):
        query_set = self._build_query(self._QUERY['reset_list'], **kwargs)
        return self._db.fetch_all_tuples(**query_set)

    # TODO will be deprecated
    def _reset_task(self, tid):
        # Build 5 reset queries
        queries = [
            self._build_sql_item(self._QUERY[name], tid=tid)
            for name in ('trash_result', 'trash_error',
                         'delete_result', 'delete_error', 'reset_task')
        ]

        self._db.execute_multiple_dml(queries)
