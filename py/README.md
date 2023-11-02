# gPS Solutions - Python Library Repository

**This is not an official Google product.**

This repository contains modules and tools useful for use with advanced data
solutions on Google Ads, Google Marketing Platform and Google Cloud. It is
maintained by a team of Customer Solutions Engineers (CSE) and Data Scientists
(DS) working within Google's Technical Solutions (gTech) Professional Services
organization (gPS).

Contributions are highly encouraged; see [CONTRIBUTING.md](../CONTRIBUTING.md).

## Installation

### For Consumers

Install via pip:

```
pip install gps-building-blocks
```

### For Contributors

The following assumes you have successfully created and started a clean
virtual environment. For more information, see the documentation for
[`venv`](https://docs.python.org/3/library/venv.html).

Install dependencies via pip:

```sh
#in gps_building_blocks root folder in the GitHub repo, clean virtualenv

cd py
pip install -r requirements.txt
```

#### Package Dependency Management

IMPORTANT: Please verify that you update any external dependencies on 
[```setup.py```](setup.py). [```requirements.txt```](requirements.txt) should be
generated from `setup.py` and _not_ edited manually. See instructions below.

[```setup.py```](setup.py) and [```requirements.txt```](requirements.txt) both
contain a list of dependencies. Both of these list serve a similar purpose, but
differ in a fundamental way. Basically, `setup.py` is used for dependency
management during deployment, and `requirements.txt` is used for development.
`setup.py` is edited manually and `requirements.txt` is automatically generated
from the contents of `setup.py` using `pip-compile`

- `setup.py`: When the user executes `pip install gps-building-blocks`,
  this package and all packages listed in setuptools.install_requires()` will
  be installed. This list should not include any libraries needed for
  development, and version requirements should generally be as generic as
  possible to reduce the chance that the end user is installing multiple
  versions of the same libraries.
- `requirements.txt`: When the user executes `pip install -r requirements.txt`,
  all packages listed in the file will be installed. This command is generally
  used when the user has cloned the project and is planning to modify it.
  Therefore, `requirements.txt` should include all packages that are necessary
  to develop and test your code. Version requirements can be as strict as
  necessary.

##### Updating dependencies
If need to import a new package or update a dependency:

 1. Add it to [```setup.py```](setup.py).
   * Make sure you pin your dependency to a particular version to ensure
     replicable builds. E.g.: `package_name==1.2.3`.
 1. Run 
`pip-compile --allow-unsafe --generate-hashes --resolver=backtracking` to update
[```requirements.txt```](requirements.txt).
 1. Test your code and submit the changes on both files.
