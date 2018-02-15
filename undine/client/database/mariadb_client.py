from collections import namedtuple
from undine.client.database.client_base import ClientBase
from undine.database.mariadb import MariaDbConnector
from undine.utils.exception import UndineException


class MariaDbClient(ClientBase):
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
        '''
    }

    _WhereItem = namedtuple('_WhereItem', ['clause', 'value'])

    _WHERE_CONDITION = {
        'mid': _WhereItem('mid = UNHEX(%(mid)s)', '{}'),
        'name': _WhereItem('name LIKE %(name)s', '%{}%'),
        'email': _WhereItem('email LIKE %(email)s', '%{}%')
    }

    #
    # Constructor & Destructor
    #
    def __init__(self, config):
        self._mariadb = MariaDbConnector(config)

    #
    # Inherited methods
    #
    def mission_list(self, list_all=False):
        where = 'WHERE complete = FALSE' if not list_all else ''
        query = self._QUERY['mission_list'].format(where=where)

        return self._mariadb.fetch_all_tuples(query)

    def mission_info(self, **kwargs):
        if 'mid' in kwargs and len(kwargs['mid']) != 32:
            raise UndineException('MID value is feet to uuid length.')

        where = list()
        values = dict()

        for key, value in kwargs.items():
            if key in self._WHERE_CONDITION:
                where.append(self._WHERE_CONDITION[key].clause)
                values[key] = self._WHERE_CONDITION[key].value.format(value)

        where = 'WHERE ' + ' AND '.join(where) if where else ''

        query = self._QUERY['mission_info'].format(where=where)

        return self._mariadb.fetch_all_tuples(query, values)
