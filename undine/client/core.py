import abc


class Connector(abc.ABC):
    @abc.abstractmethod
    def rpc_call(self, ip, command, *args, **kwargs): pass

    @abc.abstractmethod
    def mission_list(self, list_all=False): pass

    @abc.abstractmethod
    def mission_info(self, **kwargs): pass

    @abc.abstractmethod
    def task_list(self, **kwargs): pass

    @abc.abstractmethod
    def task_info(self, tid): pass

    @abc.abstractmethod
    def config_info(self, cid): pass

    @abc.abstractmethod
    def input_info(self, iid): pass

    @abc.abstractmethod
    def input_list(self): pass

    @abc.abstractmethod
    def worker_info(self, wid): pass

    @abc.abstractmethod
    def worker_list(self): pass

    @abc.abstractmethod
    def host_list(self): pass

    @abc.abstractmethod
    def cancel_tasks(self, *tasks, **kwargs): pass

    @abc.abstractmethod
    def drop_tasks(self, *tasks, **kwargs): pass

    @abc.abstractmethod
    def drop_mission(self, **kwargs): pass

    @abc.abstractmethod
    def rerun_tasks(self, *tasks, **kwargs): pass
