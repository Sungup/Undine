from collections import namedtuple
from undine.client.database.base_client import BaseClient
from undine.database.mariadb import MariaDbConnector
from undine.utils.exception import UndineException


class MariaDbClient(BaseClient):
    _QUERY = {
        'mission_list': '''
          SELECT mid, name, email,
                 ready, issued, done, canceled, failed, 
                 issued_at AS issued
            FROM mission_dashboard %s
        ORDER BY complete, issued_at DESC
        ''',
        'mission_info': '''
          SELECT HEX(mid) AS mid, name, email, description, issued
            FROM mission %s
        ORDER BY issued ASC
        ''',
        'task_list': '''
          SELECT tid, name, host, ip, state, issued, updated,
                 IF(reportable = TRUE, 'true', 'false') AS reportable
            FROM task_list %s
        ORDER BY issued ASC
        ''',
        'task_info': '''
          SELECT HEX(t.tid) AS tid, t.name AS name,
                 t.host, INET_NTOA(t.ip) AS ip, s.name AS state,
                 HEX(t.mid) AS mid,
                 HEX(t.cid) AS cid, HEX(t.iid) AS iid, HEX(t.wid) AS wid,
                 t.issued, t.updated,
                 IF(t.reportable = TRUE, 'true', 'false') AS reportable,
                 IF(r.content IS NOT NULL, r.content, '-') AS result,
                 IF(r.reported IS NOT NULL, r.reported, '-') AS succeed,
                 IF(e.message IS NOT NULL, e.message, '-') AS error,
                 IF(e.informed IS NOT NULL, e.informed, '-') AS failed 
            FROM task AS t
            JOIN state_type AS s ON t.state = s.state
            LEFT JOIN result r ON t.tid = r.tid
            LEFT JOIN error e ON t.tid = e.tid
           WHERE t.tid = UNHEX(%(tid)s)
        ''',
        'config_info': '''
          SELECT HEX(cid) AS cid, name, config, issued
            FROM config
           WHERE cid = UNHEX(%(cid)s)
        ''',
        'input_info': '''
          SELECT HEX(iid) AS iid, name, items, issued
            FROM input %s
        ORDER BY issued
        ''',
        'worker_info': '''
          SELECT HEX(wid) AS wid, name, command, arguments, worker_dir, issued
            FROM worker %s
        ORDER BY issued
        ''',
        'host_list': '''
          SELECT name, ip, issued, canceled, failed,
                 registered, logged_in, logged_out, state
            FROM host_list
        ''',

        'tid_list': '''
          SELECT HEX(tid) FROM task %s
        ''',
        'trash_result': '''
          INSERT INTO trash (tid, generated, category, content)
          SELECT tid, reported, 'result', content FROM result
           WHERE tid IN (%s)
        ''',
        'trash_error': '''
          INSERT INTO trash (tid, generated, category, content)
          SELECT tid, informed, 'error', message FROM error
           WHERE tid IN (%s)
        ''',
        'delete_result': '''
          DELETE FROM result WHERE tid IN (%s)
        ''',
        'delete_error': '''
          DELETE FROM error WHERE tid IN (%s)
        ''',
        'cancel_task': '''
          UPDATE task
             SET state = 'C', host = NULL, ip = NULL
           WHERE tid IN (%s)
        '''
    }

    _WhereItem = namedtuple('_WhereItem', ['clause', 'format'])

    _WHERE_CLAUSE = {
        'mid': _WhereItem('mid = UNHEX(%(mid)s)', '{}'),
        'tid': _WhereItem('mid = UNHEX(%(tid)s)', '{}'),
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
    def __value(form, value):
        return value if not form else form.format(value)

    @staticmethod
    def __where(condition):
        return 'WHERE ' + ' AND '.join(condition) if condition else ''

    def __query(self, template, **kwargs):
        where = list()
        params = dict()

        for k, v in kwargs.items():
            if k in self._WHERE_CLAUSE:
                where.append(self._WHERE_CLAUSE[k].clause)
                params[k] = self.__value(self._WHERE_CLAUSE[k].format, v)

        where = self.__where(where)

        return dict(query=self._QUERY[template] % where, **params)

    #
    # Inherited methods
    #
    def mission_list(self, list_all=False):
        where = 'WHERE complete = FALSE' if not list_all else ''

        return self._db.fetch_all_tuples(self._QUERY['mission_list'] % where)

    def mission_info(self, **kwargs):
        if 'mid' in kwargs and len(kwargs['mid']) != 32:
            raise UndineException('MID value must feat to uuid length.')

        query = self.__query('mission_info', **kwargs)

        return self._db.fetch_all_tuples(**query)

    def task_list(self, **kwargs):
        if 'mid' not in kwargs or len(kwargs['mid']) != 32:
            raise UndineException('MID value must feat to uuid length.')

        return self._db.fetch_all_tuples(**self.__query('task_list', **kwargs))

    def task_info(self, tid):
        if len(tid) != 32:
            raise UndineException('TID value must feat to uuid length.')

        return self._db.fetch_a_tuple(self._QUERY['task_info'], tid=tid)

    def config_info(self, cid):
        if len(cid) != 32:
            raise UndineException('CID value must feat to uuid length.')

        return self._db.fetch_a_tuple(self._QUERY['config_info'], cid=cid)

    def input_info(self, iid):
        if len(iid) != 32:
            raise UndineException('IID value must feat to uuid length.')

        return self._db.fetch_a_tuple(**self.__query('input_info', iid=iid))

    def input_list(self):
        return self._db.fetch_all_tuples(**self.__query('input_info'))

    def worker_info(self, wid):
        if len(wid) != 32:
            raise UndineException('WID value must feat to uuid length.')

        return self._db.fetch_a_tuple(**self.__query('worker_info', wid=wid))

    def worker_list(self):
        return self._db.fetch_all_tuples(**self.__query('worker_info'))

    def host_list(self):
        return self._db.fetch_all_tuples(**self.__query('host_list'))

    def _tid_list(self, **kwargs):
        return self._db.fetch_all_tuples(**self.__query('tid_list', **kwargs))

    def _cancel_task(self, *args):
        where = ", ".join(("UNHEX(%s)",) * len(args))
        queries = [
            self._db.sql(self._QUERY[name] % where, *args)
            for name in ('trash_result', 'trash_error',
                         'delete_result', 'delete_error', 'cancel_task')
        ]

        self._db.execute_multiple_dml(queries)
