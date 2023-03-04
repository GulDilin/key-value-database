import os
import pathlib
import traceback
from dataclasses import dataclass
from datetime import datetime
from io import BufferedRandom, BufferedReader
from typing import Type, TypeVar

from pydantic import BaseModel

from . import exc, types

T = TypeVar('T', bound=BaseModel)


@dataclass
class DatabaseCursor:
    db_file: str
    _DB_PREFIX: str = "key-values-database"
    _INT_SIZE: int = 64
    _META_BUFFER_SIZE: int = 512

    def __post_init__(self):
        self._DB_PREFIX_SIZE = len(self._DB_PREFIX.encode("utf-8"))
        self.db_file_path = pathlib.Path(self.db_file)
        if not self.db_file_path.parent.exists():
            os.makedirs(str(self.db_file_path.parent))
        if not self.db_file_path.exists():
            with open(self.db_file_path, "wb"):
                pass
            self.db_meta = types.MetaDB(created=datetime.now(), updated=datetime.now())
            self.write_db_meta(self.db_meta)
        else:
            self.db_meta = self.read_db_meta()

        self.tables = dict()
        self.update_all_tables_dict()

    def _encode_str(self, s: str) -> bytes:
        return s.encode("utf-8")

    def _decode_str(self, s: bytes) -> str:
        return s.decode("utf-8")

    def _encode_meta(self, meta: BaseModel) -> tuple[bytes, int]:
        b = self._encode_str(meta.json())
        return b, len(b)

    def _decode_meta(self, s: bytes, cls: Type[T]) -> T:
        return cls.parse_raw(self._decode_str(s))

    def _read_meta_size(self, f: BufferedReader | BufferedRandom) -> int:
        return int.from_bytes(f.read(self._INT_SIZE), byteorder="big", signed=False)

    def _write_meta_size(self, f: BufferedRandom, size: int) -> None:
        f.write(size.to_bytes(self._INT_SIZE, byteorder="big", signed=False))

    def _write_buffer(self, f: BufferedRandom) -> None:
        f.write(b'\x00' * self._META_BUFFER_SIZE)

    def _get_current_offset(self) -> int:
        return os.path.getsize(self.db_file_path)

    def _write_meta(
        self,
        meta: BaseModel,
        offset: int = 0,
        file: BufferedRandom | None = None,
        use_buffer: bool = False
    ) -> None:
        f = file or open(self.db_file_path, "r+b")
        f.seek(offset)
        meta_bytes, meta_size = self._encode_meta(meta)
        if use_buffer:
            self._write_buffer(f)
            f.seek(offset)
        self._write_meta_size(f, meta_size)
        f.write(meta_bytes)
        if not file:
            f.close()

    def _read_meta(self, meta_cls: Type[T], offset: int = 0, file: BufferedReader | None = None) -> T:
        f = file or open(self.db_file_path, "rb")
        f.seek(offset)
        data = self._decode_meta(f.read(self._read_meta_size(f)), meta_cls)
        if not file:
            f.close()
        return data

    def read_db_meta(self) -> types.MetaDB:
        with open(self.db_file_path, "rb") as f:
            try:
                prefix = self._decode_str(f.read(self._DB_PREFIX_SIZE))
                if prefix != self._DB_PREFIX:
                    raise exc.IncorrectDatabase()
                return self._read_meta(types.MetaDB, offset=self._DB_PREFIX_SIZE, file=f)
            except Exception:
                raise exc.IncorrectDatabase()

    def write_db_meta(self, meta: types.MetaDB) -> None:
        with open(self.db_file_path, "r+b") as f:
            f.write(self._encode_str(self._DB_PREFIX))
            self._write_meta(meta, offset=self._DB_PREFIX_SIZE, file=f, use_buffer=True)

    def update_db_meta(self, meta: types.MetaDB) -> None:
        self.write_db_meta(meta)
        self.db_meta = self.read_db_meta()

    def read_table_meta(self, offset: int) -> types.MetaTable:
        return self._read_meta(types.MetaTable, offset=offset)

    def read_all_tables(self) -> list[tuple[types.MetaTable, int]]:
        offset = self.db_meta.first_table_offset
        results = []
        while offset:
            table = self.read_table_meta(offset)
            results.append((table, offset))
            offset = table.next_table_offset
        return results

    def read_all_tables_dict(self) -> dict[str, tuple[types.MetaTable, int]]:
        return {table.name: (table, offset) for table, offset in self.read_all_tables()}

    def update_all_tables_dict(self) -> None:
        self.tables = self.read_all_tables_dict()

    def read_row_meta(self, offset: int) -> types.MetaRow:
        return self._read_meta(types.MetaRow, offset=offset)

    def read_next_row_meta(self, row: types.MetaRow) -> types.MetaRow | None:
        if not row.has_next():
            return None
        return self.read_row_meta(offset=row.next_row_offset)

    def get_table_by_name(self, table_name: str) -> types.MetaTable:
        try:
            return self.tables[table_name][0]
        except KeyError:
            raise ValueError('Table not found')

    def has_table(self, name: str) -> bool:
        return name in self.tables

    def update_table_dict(self, table: types.MetaTable, offset: int | None = None) -> None:
        if not self.has_table(table.name):
            if not offset:
                raise ValueError('Table does not cached yet, offset required')
            self.tables[table.name] = (table, offset)
        self.tables[table.name] = (table, offset or self.tables[table.name][1])

    def get_cached_table_by_offset(self, offset: int) -> types.MetaTable:
        for table, t_offset in self.tables.values():
            if t_offset == offset:
                return table
        raise ValueError('Incorrect Table Offset')

    def override_table_meta(self, table: types.MetaTable, override_table: str):
        old_meta, offset = self.tables[override_table]
        if table.name != old_meta.name and self.has_table(table.name):
            raise ValueError('Table name need to be unique')
        _, table_meta_size = self._encode_meta(table)
        if table_meta_size < self._META_BUFFER_SIZE:
            self._write_meta(table, self.tables[table.name][1], use_buffer=True)
            self.update_table_dict(table)
            return

        offset = self._get_current_offset()
        self._write_meta(table, offset, use_buffer=True)
        self.update_table_dict(table, offset)
        if old_meta.has_prev():
            prev_table = self.read_table_meta(old_meta.prev_table_offset)
            updated = prev_table.copy()
            updated.next_table_offset = offset
            self.override_table_meta(updated, prev_table.name)

        if old_meta.has_next():
            next_table = self.read_table_meta(old_meta.next_table_offset)
            updated = next_table.copy()
            updated.prev_table_offset = offset
            self.override_table_meta(updated, next_table.name)

        if not old_meta.has_prev():
            updated = self.db_meta.copy()
            updated.first_table_offset = offset
            self.update_db_meta(updated)

    def write_table_meta(self, table: types.MetaTable):
        if self.has_table(table.name):
            raise ValueError('Table name need to be unique')
        offset = self._get_current_offset()
        self._write_meta(table, offset, use_buffer=True)
        self.update_table_dict(table, offset)

        if self.db_meta.last_table_offset:
            last_table = self.get_cached_table_by_offset(self.db_meta.last_table_offset)
            last_table_updated = last_table.copy()
            last_table_updated.next_table_offset = offset
            self.override_table_meta(last_table_updated, last_table.name)
            updated = self.db_meta.copy()
            updated.last_table_offset = offset
            self.update_db_meta(updated)

        if not self.db_meta.first_table_offset:
            updated = self.db_meta.copy()
            updated.first_table_offset = offset
            updated.last_table_offset = offset
            self.update_db_meta(updated)

    def preprocess_row_data(self, table: types.MetaTable, row: types.MetaRow) -> types.MetaRow:
        if row.data.keys() != table.keys.keys():
            raise ValueError(f'Row data {row.data} is not compatible with table schema {table.keys}')
        try:
            data = {
                key: types.DB_TYPES_CONVERTERS[table.keys[key]](value)
                for key, value in row.data.items()
            }
        except Exception:
            traceback.print_exc()
            raise ValueError(f'Row data {row.data} is not compatible with table schema {table.keys}')
        row_copy = row.copy()
        row_copy.data = data
        return row_copy

    def override_row_meta(self, table_name: str, row: types.MetaRow, override_row_offset: int) -> None:
        table = self.get_table_by_name(table_name)
        row = self.preprocess_row_data(table, row)
        override_row = self.read_row_meta(override_row_offset)

        _, row_meta_size = self._encode_meta(row)
        if row_meta_size < self._META_BUFFER_SIZE:
            self._write_meta(row, override_row_offset, use_buffer=True)
            return

        offset = self._get_current_offset()
        self._write_meta(row, offset, use_buffer=True)
        if override_row.has_prev():
            prev_row = self.read_row_meta(override_row.prev_row_offset)
            updated = prev_row.copy()
            updated.next_row_offset = offset
            self.override_row_meta(table_name, updated, override_row.prev_row_offset)

        if override_row.has_next():
            next_row = self.read_row_meta(override_row.next_row_offset)
            updated = next_row.copy()
            updated.prev_row_offset = offset
            self.override_row_meta(table_name, updated, override_row.next_row_offset)

        if table.first_row_offset == override_row_offset:
            updated_table = table.copy()
            updated_table.first_row_offset = offset
            self.override_table_meta(updated_table, updated_table.name)

        if table.last_row_offset == override_row_offset:
            updated_table = table.copy()
            updated_table.last_row_offset = offset
            self.override_table_meta(updated_table, updated_table.name)

    def write_row_meta(self, table_name: str, row: types.MetaRow) -> None:
        table = self.get_table_by_name(table_name)
        row = self.preprocess_row_data(table, row)
        row.next_row_offset = 0
        offset = self._get_current_offset()

        if table.last_row_offset:
            row.prev_row_offset = table.last_row_offset
            last_row = self.read_row_meta(table.last_row_offset)
            last_row.next_row_offset = offset
            self.override_row_meta(table_name, last_row, table.last_row_offset)

        self._write_meta(row, offset, use_buffer=True)
        updated_table = table.copy()
        if not table.first_row_offset:
            updated_table.first_row_offset = offset
        updated_table.last_row_offset = offset
        self.override_table_meta(updated_table, updated_table.name)
