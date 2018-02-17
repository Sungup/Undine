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
