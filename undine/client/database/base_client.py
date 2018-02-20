from undine.utils.exception import VirtualMethodException


class BaseClient:
    def mission_list(self, list_all=False):
        raise VirtualMethodException(BaseClient, 'mission_list')

    def mission_info(self, **kwargs):
        raise VirtualMethodException(BaseClient, 'mission_info')

    def task_list(self, **kwargs):
        raise VirtualMethodException(BaseClient, 'task_list')

    def task_info(self, tid):
        raise VirtualMethodException(BaseClient, 'task_info')

    def config_info(self, cid):
        raise VirtualMethodException(BaseClient, 'config_info')

    def input_info(self, iid):
        raise VirtualMethodException(BaseClient, 'input_info')

    def input_list(self):
        raise VirtualMethodException(BaseClient, 'input_list')

    def worker_info(self, wid):
        raise VirtualMethodException(BaseClient, 'worker_info')

    def worker_list(self):
        raise VirtualMethodException(BaseClient, 'worker_list')
