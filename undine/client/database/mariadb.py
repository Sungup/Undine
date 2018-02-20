from collections import namedtuple
from undine.client.database.base_client import BaseClient
from undine.database.mariadb import MariaDbConnector
from undine.utils.exception import UndineException


class MariaDbClient(BaseClient):
    _QUERY = {
        'mission_list': '''
          SELECT mid, name, email,
                 ready, issued, done, canceled, failed, 
                 DATE_FORMAT(issued_at, '%Y-%m-%d %T')
            FROM mission_dashboard {where}
        ORDER BY complete, issued_at DESC
        ''',
        'mission_info': '''
          SELECT HEX(mid), name, email, description,
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
        self._mariadb = MariaDbConnector(config)

    #
    # Private methods
    #
    @staticmethod
    def _value(form, value):
        return value if not form else form.format(value)

    def _build_query(self, template, **kwargs):
        where = list()
        params = dict()

        for key, value in kwargs.items():
            if key in self._WHERE_CLAUSE:
                where.append(self._WHERE_CLAUSE[key].clause)
                params[key] = self._value(self._WHERE_CLAUSE[key].format, value)

        where = 'WHERE ' + ' AND '.join(where) if where else ''

        query = template.format(where=where)

        return {'query': query, 'params': params}

    #
    # Inherited methods
    #
    def mission_list(self, list_all=False):
        where = 'WHERE complete = FALSE' if not list_all else ''
        query = self._QUERY['mission_list'].format(where=where)

        return self._mariadb.fetch_all_tuples(query)

    def mission_info(self, **kwargs):
        if 'mid' in kwargs and len(kwargs['mid']) != 32:
            raise UndineException('MID value must feat to uuid length.')

        query_set = self._build_query(self._QUERY['mission_info'], **kwargs)
        return self._mariadb.fetch_all_tuples(**query_set)

    def task_list(self, **kwargs):
        if 'mid' not in kwargs or len(kwargs['mid']) != 32:
            raise UndineException('MID value must feat to uuid length.')

        query_set = self._build_query(self._QUERY['task_list'], **kwargs)
        return self._mariadb.fetch_all_tuples(**query_set)

    def task_info(self, tid):
        if len(tid) != 32:
            raise UndineException('TID value must feat to uuid length.')

        item = {'tid': tid}
        return self._mariadb.fetch_a_tuple(self._QUERY['task_info'], item)

    def config_info(self, cid):
        if len(cid) != 32:
            raise UndineException('CID value must feat to uuid length.')

        item = {'cid': cid}
        return self._mariadb.fetch_a_tuple(self._QUERY['config_info'], item)

    def input_info(self, iid):
        if len(iid) != 32:
            raise UndineException('IID value must feat to uuid length.')

        query_set = self._build_query(self._QUERY['input_info'],
                                      **{'iid': iid})
        return self._mariadb.fetch_a_tuple(**query_set)

    def input_list(self):
        query_set = self._build_query(self._QUERY['input_info'])

        print(query_set['query'])

        return self._mariadb.fetch_all_tuples(**query_set)

    def worker_info(self, wid):
        if len(wid) != 32:
            raise UndineException('WID value must feat to uuid length.')

        query_set = self._build_query(self._QUERY['worker_info'],
                                      **{'wid': wid})
        return self._mariadb.fetch_a_tuple(**query_set)

    def worker_list(self):
        query_set = self._build_query(self._QUERY['worker_info'])
        return self._mariadb.fetch_all_tuples(**query_set)
