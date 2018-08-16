from undine.client.core import Connector

import abc


class BaseView(Connector):
    _HEADER = {
        'single_item': ('Field', 'Value'),
        'mission_list': ('MID', 'Name', 'Email',
                         'Ready', 'Issued', 'Done', 'Canceled', 'Failed',
                         'Issued At'),
        'mission_info': ('MID', 'Name', 'Email', 'Description', 'Issued At'),
        'task_list': ('TID', 'Name', 'Host', 'IP', 'State',
                      'Issued', 'Updated', 'Reportable'),
        'task_info': ('TID', 'Name', 'Host', 'IP', 'State',
                      'MID', 'CID', 'IID', 'WID',
                      'Issued At', 'Updated At', 'Reportable',
                      'Result', 'Succeed At', 'Error', 'Failed At'),
        'config_info': ('CID', 'Name', 'Config', 'Issued At'),
        'input_info': ('IID', 'Name', 'Items', 'Issued At'),
        'worker_info': ('WID', 'Name', 'Command',
                        'Arguments', 'Worker Directory', 'Issued At'),
        'host_list': ('Name', 'IP', 'Issued', 'Canceled', 'Failed',
                      'Registered', 'Logged-in', 'Logged-out', 'State')
    }

    def __init__(self, connector):
        self._connector = connector

    #
    # Common bypass methods
    #
    def rpc_call(self, ip, command, *args, **kwargs):
        return self._connector.rpc_call(ip, command, *args, **kwargs)

    def cancel_tasks(self, *tasks, **kwargs):
        return self._connector.cancel_tasks(*tasks, **kwargs)

    def drop_tasks(self, *tasks, **kwargs):
        return self._connector.drop_tasks(*tasks, **kwargs)

    def drop_mission(self, **kwargs):
        return self._connector.drop_mission(**kwargs)

    def rerun_tasks(self, *tasks, **kwargs):
        return self._connector.rerun_tasks(*tasks, **kwargs)

    #
    # Abstract methods
    #
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
