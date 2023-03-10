import os
import uuid

import pytest

from app import types
from app.db import Database


def gen_db_path():
    filename = f'{uuid.uuid4()}.db-lab'
    return os.path.abspath(filename)


@pytest.fixture(autouse=True)
def db():
    filename = gen_db_path()
    db = Database(db_file=filename)
    yield db
    os.remove(filename)


def test_create_db_table(db: Database):
    tables = db.get_all_tables()
    assert tables == []

    table = types.Table(
        name=f"Test Table {uuid.uuid4()}",
        keys={'id': types.DbType.STR, 'content': types.DbType.INT},
    )
    table2 = types.Table(
        name=f"Test Table {uuid.uuid4()}",
        keys={'idx': types.DbType.STR, 'contentx': types.DbType.INT, 'column': types.DbType.INT},
    )
    db.create_table(table)
    db.create_table(table2)
    tables = db.get_all_tables()
    assert tables == [table, table2]
    tables = db.get_all_tables()
    assert tables == [table, table2]

    tables = []
    for t in db.get_tables_iterator():
        tables.append(t)
    assert tables == [table, table2]


def test_insert_row(db: Database):
    table = types.Table(
        name=f"Test Table {uuid.uuid4()}",
        keys={'id': types.DbType.STR, 'content': types.DbType.INT},
    )
    db.create_table(table)

    row_1 = types.Row(data={'id': 'aaa', 'content': 1})
    row_2 = types.Row(data={'id': 'bbb', 'content': 1})
    db.insert_row(table.name, row_1)
    db.insert_row(table.name, row_2)

    rows = []
    for row in db.get_rows_iterator(table.name):
        rows.append(row)
    print(f'{rows=}')
    assert rows == [row_1, row_2]


def test_filter_row(db: Database):
    table = types.Table(
        name=f"Test Table {uuid.uuid4()}",
        keys={'id': types.DbType.STR, 'content': types.DbType.INT},
    )
    db.create_table(table)

    row_1 = types.Row(data={'id': 'aaa', 'content': 1})
    row_2 = types.Row(data={'id': 'bbb', 'content': 1})
    db.insert_row(table.name, row_1)
    db.insert_row(table.name, row_2)

    rows = []
    for row in db.get_rows_iterator(table.name, {'id': 'aaa'}):
        rows.append(row)
    print(f'{rows=}')
    assert rows == [row_1]

    rows = []
    for row in db.get_rows_iterator(table.name, {'id': 'bbb'}):
        rows.append(row)
    print(f'{rows=}')
    assert rows == [row_2]

    rows = []
    for row in db.get_rows_iterator(table.name, {'id': 'ccc'}):
        rows.append(row)
    print(f'{rows=}')
    assert rows == []

    rows = []
    for row in db.get_rows_iterator(table.name, [{'id': 'aaa'}, {'id': 'bbb'}]):
        rows.append(row)
    print(f'{rows=}')
    assert rows == [row_1, row_2]

    rows = []
    for row in db.get_rows_iterator(table.name, {'id': ['aaa', 'bbb']}):
        rows.append(row)
    print(f'{rows=}')
    assert rows == [row_1, row_2]
