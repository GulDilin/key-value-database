from dataclasses import dataclass
from typing import Generator

from . import types
from .cursor import DatabaseCursor


@dataclass
class Database:
    db_file: str

    def __post_init__(self):
        self.cursor = DatabaseCursor(self.db_file)

    @staticmethod
    def _meta_table_to_table(meta_table: types.MetaTable) -> types.Table:
        return types.Table(
            name=meta_table.name,
            keys=meta_table.keys,
        )

    @staticmethod
    def _meta_row_to_row(meta_row: types.MetaRow) -> types.Row:
        return types.Row(
            data=meta_row.data,
        )

    def get_all_tables(self) -> list[types.Table]:
        return [
            self._meta_table_to_table(it[0])
            for it in self.cursor.read_all_tables()
        ]

    def get_table_by_name(self, name: str) -> types.Table:
        meta_table = self.cursor.get_table_by_name(name)
        return self._meta_table_to_table(meta_table)

    def get_tables_iterator(self) -> Generator[types.Table, None, None]:
        if not self.cursor.db_meta.has_tables():
            return
        meta_table = self.cursor.read_table_meta(
            self.cursor.db_meta.first_table_offset
        )
        yield self._meta_table_to_table(meta_table)
        while meta_table.has_next():
            meta_table = self.cursor.read_table_meta(meta_table.next_table_offset)
            yield self._meta_table_to_table(meta_table)

    def create_table(self, table: types.Table) -> None:
        meta_table = types.MetaTable(
            name=table.name,
            keys=table.keys,
        )
        self.cursor.write_table_meta(meta_table)

    def convert_filter_part(
        self, table: types.MetaTable, filter_part: types.FilterPart,
    ) -> types.FilterPart:
        filter_part_copy = {}
        for key in filter_part:
            if key not in table.keys:
                raise ValueError(f'Table {table.name} does not have key {key}')
            val = filter_part[key]
            if isinstance(val, list):
                val = [self.cursor.convert_db_type_value(table, key, it) for it in val]
            else:
                val = self.cursor.convert_db_type_value(table, key, val)
            filter_part_copy[key] = val
        return filter_part_copy

    def convert_filter(
        self, meta_table: types.MetaTable, filter_: types.FilterRequest,
    ) -> types.FilterRequest:
        if isinstance(filter_.filter_data, list):
            filter_copy = [
                self.convert_filter_part(meta_table, filter_part)
                for filter_part in filter_.filter_data
            ]
            return types.FilterRequest(filter_data=filter_copy)
        else:
            return types.FilterRequest(
                filter_data=self.convert_filter_part(meta_table, filter_.filter_data)
            )

    def is_row_fit_filter_val(
        self, meta_row: types.MetaRow, key: str, val: types.FilterValue
    ) -> bool:
        print(f'{meta_row.data=} {key=} {val=}')
        if isinstance(val, list):
            for v in val:
                if meta_row.data[key] == v:
                    return True
            return False
        else:
            return meta_row.data[key] == val

    def is_row_fit_filter_part(
        self, meta_row: types.MetaRow, filter_part: types.FilterPart,
    ) -> bool:
        print(f'{filter_part=}')
        for key, val in filter_part.items():
            if not self.is_row_fit_filter_val(meta_row, key, val):
                return False
        return True

    def is_row_fit_filter(
        self, meta_row: types.MetaRow, filter_: types.FilterRequest,
    ) -> bool:
        if len(filter_.filter_data) == 0:
            return True
        if isinstance(filter_.filter_data, list):
            for part in filter_.filter_data:
                if self.is_row_fit_filter_part(meta_row, part):
                    return True
            return True
        return self.is_row_fit_filter_part(meta_row, filter_.filter_data)

    def get_rows_iterator(
        self,
        table_name: str,
        filter_: types.Filter | None = None,
    ) -> Generator[types.Row, None, None]:
        meta_table = self.cursor.get_table_by_name(table_name)
        filter_copy = self.convert_filter(meta_table, types.FilterRequest(filter_data=filter_ or dict()))
        if meta_table.has_next():
            return
        meta_row = self.cursor.read_row_meta(
            meta_table.first_row_offset
        )
        if self.is_row_fit_filter(meta_row, filter_copy):
            yield self._meta_row_to_row(meta_row)
        while meta_row.has_next():
            meta_row = self.cursor.read_row_meta(meta_row.next_row_offset)
            if not self.is_row_fit_filter(meta_row, filter_copy):
                continue
            yield self._meta_row_to_row(meta_row)

    def insert_row(self, table_name: str, row: types.Row) -> None:
        meta_row = types.MetaRow(data=row.data)
        self.cursor.write_row_meta(table_name, meta_row)
