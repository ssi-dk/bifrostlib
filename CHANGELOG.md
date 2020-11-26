# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.1.0] - 2020-11-24
### Description
Changed the whole interface for datahandling from previous. It's now based on a json schema where classes from datahandling are built off the schema. The classes are extended off a base type and rely on logic from a python library called warlock. Warlock allows me to create an object off of a json schema and ensures that it is valid as an object. Right now there are minimal helper classes and the version of the schema is expected to be known that the user wants to enter data against and thus theey create the dict which the classes ensures are valid when entering them into the database. The database actions have been seperated and the datahandling class is agnostic to the database used. The datahandling class expects everything to be formatted in json. The database_interface class handles interactions with the database and converts the objects from json format into the bson format which is expected by mongodb. 

### Added
- .dockerignore
- .gitignore
- CHANGELOG.md
- LICENSE
- README.md
- requirements.txt
- bifrostlib/schemas/bifrost.jsonc
- bifrostlib/tests/test_simple.py
- bifrostlib/common.py
- bifrostlib/database_interface.py

### Changed
- .github/workflows/python_publish_pypi_and_testpypi_on_push.yml
- setup.py
- bifrostlib/datahandling.py
- bifrotslib/\_\_init\_\_.py

### Removed
- mongo_interface.py