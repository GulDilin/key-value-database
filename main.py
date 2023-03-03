import uuid

from app import types
from app.cursor import DatabaseCursor

if __name__ == "__main__":
    cursor = DatabaseCursor(db_file="xx.db-lab")
    meta = cursor.read_db_meta()
    print(f"{meta=}")
    tables = cursor.read_all_tables()
    print(f"{tables=}")
    cursor.write_table_meta(
        types.MetaTable(
            name=f"Test Table {uuid.uuid4()}",
            keys={'id': types.DbType.STR, 'content': types.DbType.INT},
        )
    )
