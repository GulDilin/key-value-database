import os
import uuid

from app import types
from app.cursor import DatabaseCursor


def gen_db_path():
    filename = f'{uuid.uuid4()}.db-lab'
    return os.path.abspath(filename)


def test_write_db_meta():
    filename = gen_db_path()
    cursor = DatabaseCursor(db_file=filename)
    meta = cursor.read_db_meta()
    print(f"{meta=}")
    assert meta == cursor.db_meta
    os.remove(filename)


def test_write_db_table():
    filename = gen_db_path()
    cursor = DatabaseCursor(db_file=filename)
    tables = cursor.read_all_tables()
    print(f"{tables=}")
    assert tables == []
    table = types.MetaTable(
        name=f"Test Table {uuid.uuid4()}",
        keys={'id': types.DbType.STR, 'content': types.DbType.INT},
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
    os.remove(filename)
