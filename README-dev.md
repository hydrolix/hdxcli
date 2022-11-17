# Install and use from a dev machine

In order to use it, you will need poetry installed.

``` shell
python3 -m pip install poetry
```

Go to the root directory of the project, where you can find a pyproject.toml
file.

There, do the following to install the environment

``` shell
python3 -m poetry shell
python3 -m poetry install
```

You can invoke the tool by doing inside your virtual environment provided by poetry. 
Assuming you are at the root of the project:

```
export PYTHONPATH=`pwd`/src
python3 -m hdx_cli.main --help
```

This adds to the Python path the root of the package

# Running tests

In order to run tests, stay at the top:


