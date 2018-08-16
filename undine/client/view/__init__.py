# for factory item
from undine.client.view.bypass import BypassView
from undine.client.view.json import JsonView
from undine.client.view.table import TableView


class ViewFactory:
    _VIEW_CLASSES = {
        'default': BypassView,
        'json': JsonView,
        'table': TableView,
    }

    @staticmethod
    def create(view_type, connector):
        if view_type in ViewFactory._VIEW_CLASSES:
            return ViewFactory._VIEW_CLASSES[view_type](connector)
        else:
            return BypassView(connector)
