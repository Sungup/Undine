from undine.utils.exception import VirtualMethodException


class Connector:
    def task_list(self, _status, **_kwargs):
        raise VirtualMethodException(Connector, 'task_list')

    def mission_stats(self, **_kwargs):
        raise VirtualMethodException(Connector, 'task_list')

    def running_list(self, **kwargs):
        return self.task_list('I', **kwargs)

    def failed_list(self, **kwargs):
        return self.task_list('F', **kwargs)

    def canceled_list(self, **kwargs):
        return self.task_list('C', **kwargs)
