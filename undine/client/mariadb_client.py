from undine.client.client_base import ClientBase


class MariaDbClient(ClientBase):
    def __init__(self, rabbitmq, _config):
        ClientBase.__init__(self, rabbitmq)
