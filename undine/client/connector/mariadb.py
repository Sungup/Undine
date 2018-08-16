from collections import namedtuple

from undine.client.connector.base_connector import BaseConnector
from undine.database.mariadb import MariaDbConnector as MariaDB


class MariaDbConnector(BaseConnector):
    __QUERY = {
        'mission_mid': '''
          SELECT mid FROM mission WHERE name = %(name)s
        ''',
        'mission_list': '''
          SELECT mid, name, email,
                 ready, issued, done, canceled, failed,
                 issued_at AS issued
            FROM mission_dashboard %s
        ORDER BY complete, issued_at DESC
        ''',
        'mission_info': '''
          SELECT mid, name, email, description, issued
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
          SELECT t.tid AS tid, t.name AS name,
                 t.host, INET_NTOA(t.ip) AS ip, s.name AS state,
                 t.mid AS mid,
                 t.cid AS cid, t.iid AS iid, t.wid AS wid,
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
           WHERE t.tid = %(tid)s
        ''',
        'config_info': '''
          SELECT cid AS cid, name, config, issued
            FROM config
           WHERE cid = %(cid)s
        ''',
        'input_info': '''
          SELECT iid AS iid, name, items, issued
            FROM input %s
        ORDER BY issued
        ''',
        'worker_info': '''
          SELECT wid AS wid, name, command, arguments, worker_dir, issued
            FROM worker %s
        ORDER BY issued
        ''',
        'host_list': '''
          SELECT name, ip, issued, canceled, failed,
                 registered, logged_in, logged_out, state
            FROM host_list
        ''',

        'tid_list': '''
          SELECT tid FROM task %s
        ''',

        # Trashing task information.
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
        ''',

        # Remove task related information.
        'delete_trash': '''
          DELETE FROM trash WHERE tid IN (%s)
        ''',
        'delete_task': '''
          DELETE FROM task WHERE tid IN (%s)
        ''',
        'delete_mission': '''
          DELETE FROM mission WHERE mid = %(mid)s
        ''',

        # Retry task
        'retry_task': '''
          UPDATE task
             SET state = 'R', host = NULL, ip = NULL
           WHERE tid IN (%s)
        '''
    }

    _WhereItem = namedtuple('_WhereItem', ['clause', 'format'])

    _WHERE_CLAUSE = {
        'mid': _WhereItem('mid = %(mid)s', '{}'),
        'tid': _WhereItem('tid = %(tid)s', '{}'),
        'cid': _WhereItem('cid = %(cid)s', '{}'),
        'iid': _WhereItem('iid = %(iid)s', '{}'),
        'wid': _WhereItem('wid = %(wid)s', '{}'),
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
        super(MariaDbConnector, self).__init__(config)

        self._db = MariaDB(self._db_config)

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

        return dict(query=self.__QUERY[template] % where, **params)

    def __manipulate_task(self, tasks, operations):
        where = ", ".join(("%s",) * len(tasks))

        self._db.execute_multiple_dml([
            self._db.sql(self.__QUERY[operation] % where, *tasks)
            for operation in operations
        ])

    #
    # Inherited methods
    #
    def _get_mid(self, name):
        item = self._db.fetch_a_tuple(self.__QUERY['mission_mid'], name=name)

        return item[0] if item else None

    def _get_tid_list(self, **kwargs):
        query_set = self.__query('tid_list', **kwargs)

        return [item[0] for item in self._db.fetch_all_tuples(**query_set)]

    def _mission_info(self, **kwargs):
        return self._db.fetch_all_tuples(**self.__query('mission_info',
                                                        **kwargs))

    def _task_list(self, **kwargs):
        return self._db.fetch_all_tuples(**self.__query('task_list', **kwargs))

    def _task_info(self, tid):
        return self._db.fetch_a_tuple(self.__QUERY['task_info'], tid=tid)

    def _config_info(self, cid):
        return self._db.fetch_a_tuple(self.__QUERY['config_info'], cid=cid)

    def _input_info(self, iid):
        return self._db.fetch_a_tuple(**self.__query('input_info', iid=iid))

    def _worker_info(self, wid):
        return self._db.fetch_a_tuple(**self.__query('worker_info', wid=wid))

    def _cancel_tasks(self, *tasks):
        # TODO Check item list size. This individual tid issue mechanism can
        # fail because of their list size. If this mechanism is not efficient,
        # add new operation using mid.
        operations = ('cancel_task',
                      'trash_result', 'trash_error',
                      'delete_result', 'delete_error')

        self.__manipulate_task(tasks, operations)

    def _drop_tasks(self, *tasks):
        # TODO Check item list size. This individual tid issue mechanism can
        # fail because of their list size. If this mechanism is not efficient,
        # add new operation using mid.
        operations = ('delete_trash',
                      'delete_result', 'delete_error',
                      'delete_task')

        self.__manipulate_task(tasks, operations)

    def _drop_mission(self, mid):
        self._db.execute_single_dml(self.__QUERY['delete_mission'], mid=mid)

    def _rerun_tasks(self, *tasks):
        # TODO Check item list size. This individual tid issue mechanism can
        # fail because of their list size. If this mechanism is not efficient,
        # add new operation using mid.
        operations = ('trash_result', 'trash_error',
                      'delete_result', 'delete_error',
                      'retry_task')

        self.__manipulate_task(tasks, operations)

    def mission_list(self, list_all=False):
        where = 'WHERE complete = FALSE' if not list_all else ''

        return self._db.fetch_all_tuples(self.__QUERY['mission_list'] % where)

    def input_list(self):
        return self._db.fetch_all_tuples(**self.__query('input_info'))

    def worker_list(self):
        return self._db.fetch_all_tuples(**self.__query('worker_info'))

    def host_list(self):
        return self._db.fetch_all_tuples(self.__QUERY['host_list'])
