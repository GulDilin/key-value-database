# Key value database with hash index

## How to run development environment

1. Install `python 3.10`
2. Install `poetry`

```shell
# Windows
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -

# *NIX
curl -sSL https://install.python-poetry.org | python3 -
```
More info at https://python-poetry.org/docs/

If you got error with python version try to add to `PYTHONPATH` python root dir (for
example `C:\Program Files\Python3.10`)

3. Check installation `poetry --version`
4. Run in project root dir `poetry shell`
5. In opened shell install dependencies with `poetry install`
6. Install pre commit hooks `pre-commit install`


## Run tests
```shell
pytest .
```
