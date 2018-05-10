from undine.utils.exception import UndineException
from undine.utils.exception import VirtualMethodException
from collections import namedtuple


class Database:
    SQLItem = namedtuple('SQLItem', ['query', 'params'])

    def sql(self, query, *args, **kwargs):
        return self.SQLItem(query, self.__check_arguments(args, kwargs))

    @staticmethod
    def __check_arguments(args, kwargs):
        if 0 < (len(args) * len(kwargs)):
            raise UndineException('Please use only one between args and kwargs')

        return args if args else kwargs if kwargs else tuple()

    #
    # Inherit functions
    #
    def _execute_multiple_dml(self, queries):
        raise VirtualMethodException(self.__class__, '_execute_multiple_dml')

    def _execute_single_dml(self, query, params):
        raise VirtualMethodException(self.__class__, '_execute_single_dml')

    def _fetch_a_tuple(self, query, params):
        raise VirtualMethodException(self.__class__, '_fetch_a_tuple')

    def _fetch_all_tuples(self, query, params):
        raise VirtualMethodException(self.__class__, '_fetch_all_tuples')

    #
    # Public functions
    #
    def execute_multiple_dml(self, queries):
        self._execute_multiple_dml(queries)

    def execute_single_dml(self, query, *args, **kwargs):
        self._execute_single_dml(query, self.__check_arguments(args, kwargs))

    def fetch_a_tuple(self, query, *args, **kwargs):
        return self._fetch_a_tuple(query,
                                   self.__check_arguments(args, kwargs))

    def fetch_all_tuples(self, query, *args, **kwargs):
        return self._fetch_all_tuples(query,
                                      self.__check_arguments(args, kwargs))
