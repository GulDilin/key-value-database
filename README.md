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
python main.py -d test-db.db-lab
```

Commands
```
usage: select [-h] --table TABLE [--limit LIMIT] [--use-index] [--all] [--counter]
              [--filter FILTER_]

options:
  -h, --help            show this help message and exit
  --table TABLE, -t TABLE
                        Table name
  --limit LIMIT, -l LIMIT
                        Rows limit
  --use-index, -i       Use indexes in select
  --all                 Do not pause select
  --counter             Only count items
  --filter FILTER_, -f FILTER_
                        [{ key: val }, ... ] or { key: val, ... }

--------


usage: create-table [-h] table

positional arguments:
  table       { name, keys: { key: type } }

options:
  -h, --help  show this help message and exit

--------


usage: create-index [-h] --table TABLE --key KEY

options:
  -h, --help            show this help message and exit
  --table TABLE, -t TABLE
                        Table name
  --key KEY, -k KEY     Table key

--------


usage: list-tables [-h]

options:
  -h, --help  show this help message and exit

--------


usage: insert [-h] --table TABLE [--data DATA]

options:
  -h, --help            show this help message and exit
  --table TABLE, -t TABLE
                        Table name
  --data DATA, -d DATA  { key: val, ... }

--------


usage: insert-auto [-h] --table TABLE [--amount AMOUNT]

options:
  -h, --help            show this help message and exit
  --table TABLE, -t TABLE
                        Table name
  --amount AMOUNT, -a AMOUNT
                        Rows amount

--------
```

Commands examples

```
create-table { name: Test, keys: { id: int, content: str } }
create-table { name: Cats, keys: { name: str, age: int, owner: str } }

list-tables

create-index -t Cats -k age
create-index -t Cats -k name
create-index -t Cats -k owner

insert -t Cats -d '{name:Kitty,age:2,owner:Lilly}'
insert -t Cats -d '{name:MurMur,age:3,owner:Lilly}'
insert -t Cats -d '{name:Pretty Cat,age:1,owner:Barry}'

insert-auto -t Cats --amount 30
insert-auto -t Cats --amount 300000

select -t Cats --counter
select -t Cats --counter --use-index
select -t Cats -f '{name: Kitty}'
select -t Cats -f '{name:Pretty Cat}'
select -t Cats -f '{age:0}' --all --use-index
select -t Cats -f '{age:0}' --all --use-index --counter
select -t Cats -f '{age:0}' --all
select -t Cats -f '{age:0}' --all --counter
select -t Cats -f '{name:Kitty}' --all --counter
select -t Cats -f '{name:Kitty}' --all --use-index
select -t Cats -f '{name:Kitty}' --all --counter --use-index

select -t Cats -f '{age:1}' --all
select -t Cats -f '{age:1}' --all --use-index
select -t Cats -f '[{age:1},{age: 2}]' --all --use-index
select -t Cats -f '[{age:1},{age: 2}]' --all
select -t Cats -f '[{age:[1, 2]},{owner: Lilly}, {age:3}]' --all --use-index --counter

select -t Cats -f '[{age:1},{age: 2}]' --counter --all --use-index
select -t Cats -f '[{age:1},{age: 2}]' --counter --all

select -t Cats --all --counter
```
