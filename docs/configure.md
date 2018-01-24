# Configuration Method

## Basic information

Undine uses JSON file for configuration file. Undine uses `config/config.json`
as the default configuration file. 

Configuration file has 3 blocks, manager, scheduler and driver. **Manager** 
block has the global to manage some directory and file extension values. 
**Scheduler** block has task scheduling values and log information. Last, 
**driver** block has the task information DB driver information.

Example of the configuration file is this.

```json
{
  "manager": {
    "config_dir": "tmp/temp/config",
    "result_dir": "tmp/temp/result",
    "result_ext": ".log",
    "input_dir": "tmp/input"
  },
  "scheduler": {
    "worker_max": 16,
    "log_file": "tmp/temp/scheduler.log",
    "log_level": "info"
  },
  "driver": {
    "type": "file",
    "config_ext": ".json",
    "config_file": "example/task-config.csv",
    "input_file": "example/task-inputs.csv",
    "result_dir": "tmp/example/result",
    "result_ext": ".log",
    "worker_command": "example.py",
    "worker_arguments": "-c %C -r %R %I",
    "worker_dir": "example",
    "log_file": "tmp/temp/driver.log",
    "log_level": "info"
  }
}
```

## Manager

Manager block has running environment values, temporary directory path and 
input file home directory.

- `config_dir`: Temporary configure file home directory for each commandline 
  program. All commandline tasks use a configure file in this directory. But,
  the configuration file must be deleted if there is no tasks using that.
- `result_dir`: Temporary result file home directory from each commandline 
  program. All commandline tasks must store a result into a file in this 
  directory, and task manager load that file to store into the database.
- `result_ext`: File extension for the temporary result file.
- `input_dir`: Input files home directory. All tasks should use one more file
   in this directory to run a task. 

## Scheduler

Scheduler block has some valued to manage the system resource. Currently, 
scheduler manage the number of cpu only. Also, this block has log information
to check the task state.

- `worker_max`: Total number of worker count concurrently. If this values is 
  greater than the number of cpu on system, scheduler reduces the workers to 
  prevent cpu race condition.
- `log_file`: Task state logging file path.
- `log_level`: Task logging level. Default value is ERROR.

## Driver

Driver block has database or file path information to manage the task 
information. Currently it has only file driver information, but it will extend
the other database like mariadb or sqlite.

- `type`: Driver type. Currently support only `file` for FileDriver.
- `log_file`: Driver information logging file path.
- `log_level`: Driver information logging level. Default value is ERROR.

### File Driver

- `config_file`: Configure information list file.
- `input_file`: Input set information list file.
- `config_ext`: Configure file extension.
- `result_dir`: Result file home directory.
- `result_ext`: File extension of result file.
- `worker_command`: Task worker's commandline tool.
- `worker_arguments`: Task worker's commandline arguments.
- `worker_dir`: Task worker's parent directory.