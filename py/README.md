# gPS Solutions - Core Libraries Repository

go/gPS-BB-doc

**Note:** This is the internal documentation for the gps_building_blocks
repository and should be used for any content that we do not want to publicize.
The external documentation can be found [here](EXTERNAL_README.md)
and should only contain external-friendly information pertaining to the
repository contents.

## Pypi Package Release Pipeline

The gPS Building Blocks python package is hosted on the Python Package Index
([PyPI](https://pypi.org/project/gps-building-blocks/)), with pacakage updates
controlled through a [Travis CI pipeline](../.travis.yml).

## Installation

Install via pip:

```
pip install gps-building-blocks
```

## Package Dependency

[```setup.py```](setup.py) and [```requirements.txt```](requirements.txt) both
contain a list of dependencies. Both of these list serve a similar purpose, but
differ in a fundamental way. Basically, `setup.py` is used for dependency
management during deployment, and `requirements.txt` is used for development.

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

TLDR: If your code imports a new package, please add it to both
[```setup.py```](setup.py) and [```requirements.txt```](requirements.txt).



