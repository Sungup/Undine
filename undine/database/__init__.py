from undine.utils.exception import UndineException
from collections import namedtuple

import abc


class Database(abc.ABC):
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
    @abc.abstractmethod
    def _execute_multiple_dml(self, queries): pass

    @abc.abstractmethod
    def _execute_single_dml(self, query, params): pass

    @abc.abstractmethod
    def _fetch_a_tuple(self, query, params): pass

    @abc.abstractmethod
    def _fetch_all_tuples(self, query, params): pass

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
