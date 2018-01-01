# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- Add documentation file.

### Changed

### Removed

## 0.0.1 - 2018.01.01

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
  
[Unreleased]: /../compare/v0.0.1...HEAD
