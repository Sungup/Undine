from os import makedirs, path
from undine.driver.driver_base import DriverBase
from undine.utils.exception import UndineException
from undine.information import ConfigInfo, WorkerInfo, InputInfo, TaskInfo


class FileDriver(DriverBase):
    _DEFAULT_RESULT_DIR = 'results'
    _DEFAULT_RESULT_EXT = '.log'
    _DEFAULT_WORKER_ID = 0
    _DEFAULT_WORKER_CMD = 'example.rb'
    _DEFAULT_WORKER_ARGS = '-c %C -r %R %I'
    _DEFAULT_WORKER_DIR = ''

    #
    # Private/Protected methods
    #
    def _load_config(self, file_path):
        self._configs = dict()

        cid = 0
        for line in open(file_path, 'r'):
            if not line.strip():
                continue

            name, config = [item.strip()
                            for item in line.split(',', maxsplit=1)]

            self._configs[cid] = ConfigInfo(cid=cid, name=name, config=config,
                                            dir=self._config_dir,
                                            ext=self._config_ext)

            cid += 1

    def _load_inputs(self, file_path):
        self._inputs = dict()

        iid = 0
        for line in open(file_path, 'r'):
            if not line.strip():
                continue

            name, items = [item.strip()
                           for item in line.split(',', maxsplit=1)]

            self._inputs[iid] = InputInfo(iid=iid, name=name, items=items)

            iid += 1

    def _build_worker(self, config):
        command = config.setdefault('worker_command', self._DEFAULT_WORKER_CMD)
        args = config.setdefault('worker_arguments', self._DEFAULT_WORKER_ARGS)
        directory = config.setdefault('worker_dir', self._DEFAULT_WORKER_DIR)

        self._worker = WorkerInfo(wid=self._DEFAULT_WORKER_ID,
                                  dir=directory, cmd=command, arguments=args)

    def _build_task_matrix(self):
        # key: tid, value: TaskInfo
        self._tasks = dict()

        # key: state name, value: tid list
        self._state = {'ready': list(), 'issued': list(), 'done': list(),
                       'canceled': list(), 'failed': list()}

        total_inputs = len(self._inputs)
        for cid in self._configs.keys():
            for iid in self._inputs.keys():
                tid = cid * total_inputs + iid

                self._tasks[tid] = TaskInfo(tid=tid, cid=cid, iid=iid,
                                            wid=self._DEFAULT_WORKER_ID)

                self._state['ready'].append(tid)

    def _task_name(self, task):
        return "tid{0}-{1}-{2}".format(task.tid,
                                       self._configs[task.cid].name,
                                       self._inputs[task.iid].name)

    def _task_filename(self, task, ext):
        return "{0}{1}".format(self._task_name(task), ext)

    def _move_state(self, tid, from_, to_):
        self._state[from_].remove(tid)
        self._state[to_].append(tid)

        return self._tasks[tid]

    #
    # Constructor & Destructor
    #
    def __init__(self, config, config_dir):
        DriverBase.__init__(self, config, config_dir)

        # 1. Check input parameter is not missing
        if 'config_file' not in config:
            raise UndineException("'config_file' is not set in driver section")

        if 'input_file' not in config:
            raise UndineException("'input_file' is not set in driver section")

        # 2. Get default values
        self._result_ext = config.setdefault('result_ext',
                                             self._DEFAULT_RESULT_EXT)

        # 3. Load task information
        self._load_config(config['config_file'])
        self._load_inputs(config['input_file'])
        self._build_worker(config)
        self._build_task_matrix()

        # 4. Make result repository
        self._results = dict()
        self._result_dir = config.setdefault('result_dir',
                                             self._DEFAULT_RESULT_DIR)

        makedirs(self._result_dir, mode=0o700, exist_ok=True)

    #
    # Inherited methods
    #
    def fetch(self):
        return self._tasks[self._state['ready'][0]]

    def config(self, cid):
        return self._configs[cid]

    def worker(self, _wid):
        return self._worker

    def inputs(self, iid):
        return self._inputs[iid]

    def preempt(self, tid):
        self._move_state(tid, 'ready', 'issued')

        return True

    def done(self, tid, contents):
        task = self._move_state(tid, 'issued', 'done')

        # Keep result in memory currently.
        self._results[tid] = contents

        # Store result file in file repository
        filename = self._task_filename(task, self._result_ext)
        with open(path.join(self._result_dir, filename), 'w') as f_out:
            f_out.write(contents)

        return True

    def cancel(self, tid):
        self._move_state(tid, 'issued', 'canceled')

    def fail(self, tid, message):
        task = self._move_state(tid, 'issued', 'failed')
        title = 'tid({1}) - {0}'.format(self._task_name(task), tid)

        self._error_logging(title, message)

    def wait_others(self):
        return bool(self._state['ready'])
