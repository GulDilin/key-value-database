import hashlib
import json
from dataclasses import dataclass, field
from itertools import chain
from typing import Any, Generator

from . import types
from .cursor import DatabaseCursor


@dataclass
class Indexer:
    cursor: DatabaseCursor
    # { table_name: { key: { hash: [ offset, ... ] } } }
    index_dict: dict[str, dict[str, dict[str, list[int]]]] = field(default_factory=dict)

    @staticmethod
    def get_md_5_bytes_hash(bytes):
        result = hashlib.md5(bytes).hexdigest()
        return result

    @staticmethod
    def hash(it: Any):
        return Indexer.get_md_5_bytes_hash(str(it).encode('utf-8'))

    def _add_val(self, meta_table: types.MetaTable, key: str, meta_row: types.MetaRow, row_offset: int):
        if meta_table.name not in self.index_dict:
            self.index_dict[meta_table.name] = {}
        if key not in self.index_dict[meta_table.name]:
            self.index_dict[meta_table.name][key] = {}
        hash_v = self.hash(meta_row.data[key])
        if hash_v not in self.index_dict[meta_table.name][key]:
            self.index_dict[meta_table.name][key][hash_v] = []
        if row_offset not in self.index_dict[meta_table.name][key][hash_v]:
            self.index_dict[meta_table.name][key][hash_v].append(row_offset)

    def add_item(self, meta_table: types.MetaTable, meta_row: types.MetaRow, row_offset: int):
        for key in meta_table.indexes:
            self._add_val(meta_table, key, meta_row, row_offset)

    def get_offsets_for(self, meta_table: types.MetaTable, key: str, value: Any):
        if key not in self.index_dict[meta_table.name]:
            raise ValueError(f'Index for key {key} in table {meta_table.name} does not exists')
        hash_v = self.hash(value)
        return self.index_dict[meta_table.name][key].get(hash_v, [])

    def build_for_table(self, table_name: str):
        meta_table = self.cursor.get_table_by_name(table_name)
        offset = meta_table.first_row_offset
        while offset:
            meta_row = self.cursor.read_row_meta(offset)
            self.add_item(meta_table, meta_row, offset)
            offset = meta_row.next_row_offset

    def build_for_table_key(self, table_name: str, key: str):
        meta_table = self.cursor.get_table_by_name(table_name)
        if key not in meta_table.keys:
            raise ValueError(f'Key {key} does not present in table {table_name}')
        offset = meta_table.first_row_offset
        while offset:
            meta_row = self.cursor.read_row_meta(offset)
            self._add_val(meta_table, key, meta_row, offset)
            offset = meta_row.next_row_offset

    def save(self):
        print('Saving index to file')
        with open(f'{self.cursor.db_file}.index.json', 'w') as f:
            json.dump(self.index_dict, f, indent=2)

    def load(self):
        print('Loading index from file')
        with open(f'{self.cursor.db_file}.index.json', 'r') as f:
            self.index_dict = json.load(f)
        print('Index loaded')

    def get_filter_keys_for_indexes(
        self,
        meta_table: types.MetaTable,
        filter_: types.Filter,
    ) -> set[str]:
        filter_keys = set(chain(*[part.keys() for part in filter_])) \
            if isinstance(filter_, list) else set(filter_.keys())
        for key in filter_keys:
            if key not in meta_table.indexes:
                raise ValueError(f'Index for key {key} does not present in table {meta_table.name}')
        return filter_keys

    def get_filter_part_val_indexes_offsets(
        self,
        meta_table: types.MetaTable,
        key: str,
        value: types.FilterValue,
    ) -> set[int]:
        result = set()
        if isinstance(value, list):
            for v in value:
                result = result.union(self.get_offsets_for(meta_table, key, v))
        else:
            result = set(self.get_offsets_for(meta_table, key, value))
        return result

    def get_filter_part_indexes_offsets(
        self,
        meta_table: types.MetaTable,
        filter_part: types.FilterPart,
    ) -> set[int]:
        result = None
        for key, value in filter_part.items():
            sub = self.get_filter_part_val_indexes_offsets(meta_table, key, value)
            if result is None:
                result = sub
            continue
            result = result.intersection(sub)
        return result or set()

    def get_filter_indexes_offsets(
        self,
        meta_table: types.MetaTable,
        filter_: types.Filter,
    ) -> set[int]:
        self.get_filter_keys_for_indexes(meta_table, filter_)
        result = set()
        if isinstance(filter_, list):
            for filter_part in filter_:
                result = result.union(self.get_filter_part_indexes_offsets(meta_table, filter_part))
        else:
            result = self.get_filter_part_indexes_offsets(meta_table, filter_)
        return result

    def get_rows_iterator_use_indexes(
        self,
        table_name: str,
        filter_: types.Filter,
    ) -> Generator[types.MetaRow, None, None]:
        meta_table = self.cursor.get_table_by_name(table_name)
        offsets = self.get_filter_indexes_offsets(meta_table, filter_)
        for offset in offsets:
            meta_row = self.cursor.read_row_meta(offset)
            yield meta_row
