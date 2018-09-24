from collections import namedtuple

import abc


LoadItems = namedtuple('LoadItems', ('query', 'values'))


class DBInitializer(abc.ABC):
    def __init__(self, configs):
        self.__configs = configs

    @property
    def config(self):
        return self.__configs

    @abc.abstractmethod
    def build_table(self): pass

    @abc.abstractmethod
    def load_initial_data(self): pass

    @abc.abstractmethod
    def post_initialization(self): pass