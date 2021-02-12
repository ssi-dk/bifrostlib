# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.1.6] - 2020-02-12
### Changed
- bifrostlib/schemas/bifrost.jsonc
  - Add boolean to test schema

## [2.1.5] - 2020-02-11
### Changed
- common.py
  - fixed a bug related to save_yaml not working
## [2.1.4] - 2020-01-11
Had to debug the setup.cfg with a wrong path

### Changed
- setup.cfg
- schema/bifrost.jsonc 
  - fixed a bug related to potential shorter timestamps which emerge

## [2.1.3] - 2020-01-11
See 2.1.4
## [2.1.2] - 2021-01-04
### Description
Added a new datatype "Test" for stamper and a function in common 'set_status_and_save' which sets and saves status for both sample and sample_component
### Changed
- bifrostlib\common.py
- bifrostlib\datahandling.py

## [2.1.1] - 2020-12-07
### Description
Changes related to datahandling creation of an object from a reference and creation of a reference. Changed them to class methods. Briefly explored the concept of using \_\_new\_\_ to create them instead but considered this method to be more intuitive.
### Changed
- Dockerfile
  - minor adjustments on caching
- datahandling.py
  - Changed how loading worked to be a class method meaning running the load function returns a object of the associated type. In doing so a reference is associated with loading and not with the object creation anymore. To do this the object type was changed to a class property and the according change done in reference and object types. Generating a reference now also works in a similar fashion.
- tests/test_simple.py
  - Adjusted due to changes in datahandling.py
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
- tests/test_simple.py
- bifrostlib/schemas/bifrost.jsonc
- bifrostlib/common.py
- bifrostlib/database_interface.py

### Changed
- .github/workflows/python_publish_pypi_and_testpypi_on_push.yml
- setup.py
- bifrostlib/datahandling.py
- bifrotslib/\_\_init\_\_.py

### Removed
- mongo_interface.py