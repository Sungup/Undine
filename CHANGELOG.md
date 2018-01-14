# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

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

### Removed

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
  
[Unreleased]: /../compare/v0.0.4...HEAD
[v0.0.4]: /../compare/v0.0.3...v0.0.4
[v0.0.3]: /../compare/v0.0.2...v0.0.3
[v0.0.2]: /../compare/v0.0.2...v0.0.1
