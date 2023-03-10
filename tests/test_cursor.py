import os
import uuid

import pytest

from app import types
from app.cursor import DatabaseCursor


def pytest_namespace():
    return {'cursor': None}


def gen_db_path():
    filename = f'{uuid.uuid4()}.db-lab'
    return os.path.abspath(filename)


@pytest.fixture(autouse=True)
def cursor():
    filename = gen_db_path()
    cursor = DatabaseCursor(db_file=filename)
    yield cursor
    os.remove(filename)


def test_write_db_meta(cursor: DatabaseCursor):
    meta = cursor.read_db_meta()
    print(f"{meta=}")
    assert meta == cursor.db_meta


def test_write_db_table(cursor: DatabaseCursor):
    tables = cursor.read_all_tables()
    print(f"{tables=}")
    assert tables == []
    table = types.MetaTable(
        name=f"Test Table {uuid.uuid4()}",
        keys={'id': types.DbType.STR, 'content': types.DbType.INT},
        indexes=[]
    )
    cursor.write_table_meta(table)
    assert table.name in cursor.tables
    cache_table, offset = cursor.tables[table.name]
    assert cache_table == table

    print(f'{cursor.db_meta.first_table_offset=}')
    assert offset == cursor.db_meta.first_table_offset
    tables = cursor.read_all_tables()
    print(f'{tables[0]=}')
    assert table == tables[0][0]
    assert offset == tables[0][1]
    assert len(tables) == 1
    db_table = cursor.read_table_meta(offset)
    assert db_table == table


def test_write_2_db_tables(cursor: DatabaseCursor):
    tables = cursor.read_all_tables()
    print(f"{tables=}")
    assert tables == []
    table = types.MetaTable(
        name=f"Test Table {uuid.uuid4()}",
        keys={'id': types.DbType.STR, 'content': types.DbType.INT},
        indexes=[]
    )
    table2 = types.MetaTable(
        name=f"Test Table {uuid.uuid4()}",
        keys={'idx': types.DbType.STR, 'contentx': types.DbType.INT, 'column': types.DbType.INT},
        indexes=[]
    )
    cursor.write_table_meta(table)
    cursor.write_table_meta(table2)

    tables = cursor.read_all_tables()
    assert len(tables) == 2
    assert tables[0][0].name == table.name
    assert tables[0][0].keys == table.keys
    assert tables[1][0].name == table2.name
    assert tables[1][0].keys == table2.keys
    assert cursor.get_table_by_name(table.name).keys == table.keys
    assert cursor.get_table_by_name(table2.name).keys == table2.keys
    assert cursor.db_meta.first_table_offset == tables[0][1]
    assert cursor.db_meta.last_table_offset == tables[1][1]


def test_write_row(cursor: DatabaseCursor):
    table = types.MetaTable(
        name=f"Test Table {uuid.uuid4()}",
        keys={'id': types.DbType.STR, 'content': types.DbType.INT},
        indexes=[]
    )
    cursor.write_table_meta(table)

    row = types.MetaRow(data={'id': 'aaa', 'content': 1})
    cursor.write_row_meta(table_name=table.name, row=row)
    first_row_offset = cursor.get_table_by_name(table.name).first_row_offset
    assert first_row_offset > 0
    r_row = cursor.read_row_meta(first_row_offset)
    assert r_row.data == r_row.data


def test_write_2_row(cursor: DatabaseCursor):
    table = types.MetaTable(
        name=f"Test Table {uuid.uuid4()}",
        keys={'id': types.DbType.STR, 'content': types.DbType.INT},
        indexes=[]
    )
    cursor.write_table_meta(table)

    row = types.MetaRow(data={'id': 'aaa', 'content': 1})
    row2 = types.MetaRow(data={'id': 'bbb', 'content': 2})
    cursor.write_row_meta(table_name=table.name, row=row)
    print(f'db_table={cursor.get_table_by_name(table.name)}')
    cursor.write_row_meta(table_name=table.name, row=row2)
    print(f'db_table={cursor.get_table_by_name(table.name)}')

    db_table = cursor.get_table_by_name(table.name)
    assert db_table.first_row_offset > 0
    assert db_table.last_row_offset > 0
    assert db_table.last_row_offset > db_table.first_row_offset
    r_row = cursor.read_row_meta(db_table.first_row_offset)
    print(f'{r_row=}')
    r_row_2 = cursor.read_row_meta(r_row.next_row_offset)
    print(f'{r_row_2=}')
    assert r_row.data == r_row.data
    assert r_row_2.data == r_row_2.data

    assert r_row.prev_row_offset == 0
    assert r_row.next_row_offset == db_table.last_row_offset
    assert r_row_2.prev_row_offset == db_table.first_row_offset
    assert r_row_2.next_row_offset == 0
