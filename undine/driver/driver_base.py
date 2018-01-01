from undine.utils.exception import UndineException


class DriverBase:
    def __init__(self, config_dir):
        self._config_dir = config_dir

    def fetch(self):
        raise UndineException('This method is the abstract method of fetch')

    def config(self, _cid):
        raise UndineException('This method is the abstract method of config')

    def worker(self, _wid):
        raise UndineException('This method is the abstract method of worker')

    def inputs(self, _iid):
        raise UndineException('This method is the abstract method of inputs')

    def preempt(self, _tid):
        raise UndineException('This method is the abstract method of preempt')

    def done(self, _tid, _contents):
        raise UndineException('This method is the abstract method of task done')

    def cancel(self, _tid):
        raise UndineException('This method is the abstract method of cancel')

    def fail(self, _tid, _message):
        raise UndineException('This method is the abstract method of fail')

    def wait_others(self):
        raise UndineException('This method is the abstract method for '
                              'instance termination')
