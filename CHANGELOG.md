# Changelog

All notable changes to this project will be documented in this file.

## [v0.1.0]

### Added

- Add commandline script `undine_cli.py` and `undine_svr.py` to run command 
  simply.
- Add command line functions. Cancel, remove and re-run.
  - Task and mission command can support three commands.
  - But, this sub-commands need to be optimized near future.
- Add set-up document using docker environment on linux.

### Changed

- Change the database connector interface.
  - Add `undine.database.base` as a interface of db connector.
  - Now support the positional and key-word arguments.
- Change abstract functions in `BaseNetworkDriver`
- Change introduction pages.
- Change the field type of uuid in mariadb from BINARY(16) to CHAR(32)
  synchronizing with django based webapp.
- Change relation ship between `client`, `wrapper`, and `connector`.
  - Change name `client` to `connector` and `wrapper` to `view`.
  - Change pattern to decorator pattern.
- Apply ABC classes in `undine.database` and `undine.client`
- Change published attribute to message queue using only tid.
- Fix some bugs.

### Removed
- Remove and re-design reset tasks in command line.

## [v0.0.7]

### Added

- Add host table and its manipulation features on `undine.server`
  - Move the ghost task, issued but not running, into cancel state at server 
    log in time.
  - Add host CLI command.
  - Add reset task interface but not yet implemented.
- Print host information on dashboard.

### Changed

- Refactoring some files and class names
  - Move `undine.driver` and `undine.rpc` into `undine.server`
  - Rename some abstract class name and files.  
    `ex) ClassBase -> BaseClass, file_driver.py -> file.py`
  - Move factory class into `__init__.py` of each package directory.
  - Move database wrapper for cli client into `undine.client.database.wrapper`
  - Merge `network_client.py` into `base_client`
  - Add new RPC command `list`

## [v0.0.6]

### Added

- Add `ISSUE_TEMPLATE.md` and `PULL_REQUEST_TEMPLATE.md`
- Add CLI features.
  - Currently support dashboard, mission lookup, and task lookup commands.
  - Will support additional lookup features for config, input, and worker.
  - `client.command`: Command interface module for CLI client.
    - `command_factory.py`: Factory module.
    - `dashboard.py`: Dashboard module.
    - `mission.py`: Mission lookup module.
    - `task.py`: Task and its attribute lookup module.
      - `Task`: Task lookup command class of the specific mission.
      - `Config`: Config lookup command.
      - `Input`: Input lookup command.
      - `Worker`: Worker lookup command.
  - `client.database`: CLI client specific database connection module.
    - Base class
      - `client_base`: Interface class for CLI client database connector.
      - `client_factory`: CLI client database connector factory.
    - Database class
      - `mariadb_client`: MariaDB client database connector.
    - Wrapper class
      - `wrapper_base`: Output interface class for CLI.
      - `cli_wrapper`: ASCII-Table based output wrapper.
      - `json_wrapper`: JSON based output wrapper.

### Changed

- RabbitMQ RPC
  - Add multiple argument feature for rabbitmq rpc.
  - Add queue existence checking feature.
- Change module paths to add CLI module.
  - Move old `client` module move into `api.database` module.
  - Move file on root module into `core` and `server` module. 
  
## [v0.0.5]

### Added

- Add rpc features.
  - `rpc_daemon.py`: RPC Daemon for undine's internal status.
  - Add RPC Server/Client of RabbitMQ in `rabbitmq.py`
  - Add RPC daemon field in configuration file.
- Add exception handler on TaskManager's loop field to call the destructor while
  termination.
- Add `mission`, the meaning is a set of tasks, in the client sdk side.
- Add selective report for each task.
- Add code of conduct at docs directory.

### Changed

- Add host information field for network_driver_base and its children.
  - Redesign the NetworkDriverBase and it's children.
  - Add `host` and `ip` field in task table in database.
  - Change the host information utility for `System` class.
- Rename the field of rabbitmq to task_queue to add new rabbitmq configuration
  of RPC queue.
- Add `is_ready()` method to scheduler to prevent the long pending start.
- Change the method `wait_others()` in DriverBase to `is_ready()`
- Fixed registering the ghost RPC functions.

### Removed

- Remove `undine/client/client_factory.py`

## [v0.0.4] - 2018.01.14

### Added

- Add database connector.
  - To share the database manipulate source, decoupled that source from each 
    driver.
    - `sqlite.py`: SQLite manipulate class
    - `mariadb.py`: MariaDB manipulate class
    - `rabbitmq.py`: RabbitMQ manipulate class
- Add client interface sdk classes.
  - To some tasks and its information, add new package and classes for each 
    database. This package structure is very similar with `undine.driver`
    - `client_factorypy`: Client interface factory class. But not in use 
      currently
    - `client_base.py`: Client interface base class.
    - `network_client_base.py`: Client interface base class for the remote task 
      database.
    - `sqlite_client.py`: Client interface class for sqlite
    - `mariadb_client.py`; Client interface class for mariadb

### Changed

- Change database manipulation mechanism in `MariaDbDriver`, `SQLiteDriver`, and
  `NetworkDriverBase` class.
- Also, change example database builder using the client interface sdk classes. 

## [v0.0.3] - 2018.01.13

### Added

- Add some drivers.
  - `network_driver_base.py`: Base class for the remote task database.
    The network_driver_base manage the rabbitmq message queue internally.
  - `mariadb_driver.py`: Add mariadb Driver
    - `example_mariadb_build.py`: Example mariadb database initializer
- Now support RabbitMQ message queue
  - To support multiple undine task worker through the internal network.

### Changed

- Change each id field in database from int to text or byte(32)
- Move some object variables between child and parent in driver
- Add mutex locking mechanism in the sqlite driver to avoid the runtime error
- Change the id type of each Info class from int to object in `information.py`
- Add new basic config file for the mariadb

## [v0.0.2] - 2018.01.09

### Added

- Add some drivers.
  - `json_driver.py`: Add JSON Driver
  - `sqlite_driver.py`: Add SQLite3 Driver
    - `example_sqlite3_build`: Example SQLite3 database file generator

- Add document file.

### Changed

- Move example config file into each sub-directory
- Add new basic config file for the SQLite3

## v0.0.1 - 2018.01.01

### Added

- Initial commit for Undine.

- Add main modules
  - `__main__.py`: Main Undine python module.
  - `information.py`: Task information class module.
  - `task.py`: Commandline tack object class.
  - `process.py`: Commandline task scheduler and thread management class.

- Add driver modules
  - Currently support only file based data driver. DBMS based data driver will
    be support near future.
  - `driver_base.py`: Abstract class of driver classes. It is currently support
    interface functions and logging mechanism.
  - `file_driver.py`: File based data driver. This driver uses two list files,
    input list file and config list file.
  - `driver_factory.py`: Factory pattern class to build detail driver object.
    But, currently supports only FileDriver.

- Add utility modules
  - `exception.py`: Basic exception class for Undine project.
  - `logging.py`: Python's logging wrapper to simplify usage.
  - `path.py`: Simple file path generator and file creator.
  - `system.py`: System level information query and utilities.

- Add example commandline tool and environment setup script
  - `example.py`: Simple commandline script to check debug.
  - `example_setup.py`: Example environment builder script.
  - `task-config.csv`: Example config information file for file driver.
  - `task-input.csv`: Example input information file for file driver.
 
- Add basic config file
  - `config/config.json`: Default configure file of Undine.
  
[Unreleased]: /../compare/v0.1.0...HEAD
[v0.1.0]: /../compare/v0.0.7...v0.1.0
[v0.0.7]: /../compare/v0.0.6...v0.0.7
[v0.0.6]: /../compare/v0.0.5...v0.0.6
[v0.0.5]: /../compare/v0.0.4...v0.0.5
[v0.0.4]: /../compare/v0.0.3...v0.0.4
[v0.0.3]: /../compare/v0.0.2...v0.0.3
[v0.0.2]: /../compare/v0.0.2...v0.0.1
