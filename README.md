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

## Use db shell
```
python main.py

create-table { "name": "Test", "keys": { "id": "int", "content": "str" } }
create-table { "name": "Cats", "keys": { "name": "str", "age": "int", "owner": "str" } }

insert -t;Cats;-d;{"name":"Kitty","age":2,"owner":"Lilly"}
insert -t;Cats;-d;{"name":"MurMur","age":3,"owner":"Lilly"}
insert -t;Cats;-d;{"name":"Pretty Cat","age":1,"owner":"Barry"}

select -t;Cats;-f;{"name":"MurMur"}
select -t;Cats;-f;{"name":"Pretty Cat"}
select -t;Cats;-f;{"age":"0"};--all
select -t;Cats;-f;{"name":"Kitty"};--all;--counter


insert-auto -t;Cats;--amount;30
select -t;Cats;--all;--counter
```
