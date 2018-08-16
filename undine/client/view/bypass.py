from undine.client.view.base_view import BaseView


#
# Simple by viewer
#
class BypassView(BaseView):
    def __init__(self, connector):
        super(BypassView, self).__init__(connector)

    def mission_list(self, list_all=False):
        return self._connector.mission_list(list_all)

    def mission_info(self, **kwargs):
        return self._connector.mission_info(**kwargs)

    def task_list(self, **kwargs):
        return self._connector.task_list(**kwargs)

    def task_info(self, tid):
        return self._connector.task_info(tid)

    def config_info(self, cid):
        return self._connector.config_info(cid)

    def input_info(self, iid):
        return self._connector.input_info(iid)

    def input_list(self):
        return self._connector.input_list()

    def worker_info(self, wid):
        return self._connector.worker_info(wid)

    def worker_list(self):
        return self._connector.worker_list()

    def host_list(self):
        return self._connector.host_list()
