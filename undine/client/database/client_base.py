from undine.utils.exception import VirtualMethodException


class ClientBase:
    def mission_list(self, list_all=False):
        raise VirtualMethodException(ClientBase, 'mission_list')

    def mission_info(self, **kwargs):
        raise VirtualMethodException(ClientBase, 'mission_info')

    def task_list(self, **kwargs):
        raise VirtualMethodException(ClientBase, 'task_list')

    def task_info(self, tid):
        raise VirtualMethodException(ClientBase, 'task_info')

    def config_info(self, cid):
        raise VirtualMethodException(ClientBase, 'config_info')

    def input_info(self, iid):
        raise VirtualMethodException(ClientBase, 'input_info')

    def input_list(self):
        raise VirtualMethodException(ClientBase, 'input_list')

    def worker_info(self, wid):
        raise VirtualMethodException(ClientBase, 'worker_info')

    def worker_list(self):
        raise VirtualMethodException(ClientBase, 'worker_list')
