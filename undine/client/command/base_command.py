import abc


class BaseCommand(abc.ABC):
    def __init__(self, config, connector):
        self._connector = connector
        self._config = config

    @staticmethod
    @abc.abstractmethod
    def help(): pass

    @staticmethod
    @abc.abstractmethod
    def add_arguments(parser): pass

    @abc.abstractmethod
    def run(self): pass
