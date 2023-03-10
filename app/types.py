from datetime import datetime
from enum import Enum
from typing import Any, Callable

from pydantic import BaseModel


class ValuesEnum(Enum):
    @classmethod
    def values(cls) -> list[Any]:
        return [it.value for it in cls]

    @classmethod
    def from_value(cls, val: Any) -> Any:
        return [it for it in cls if it.value == val][0]

    def __str__(self) -> str:
        return str(self.value)


class StrEnum(str, ValuesEnum):
    pass


class DbType(StrEnum):
    INT = "int"
    STR = "str"


DB_TYPES_CONVERTERS: dict[DbType, Callable] = {
    DbType.INT: int,
    DbType.STR: str,
}


class MetaDB(BaseModel):
    created: datetime
    updated: datetime
    first_table_offset: int = 0
    last_table_offset: int = 0

    def has_tables(self):
        return self.first_table_offset > 0


class MetaKey(BaseModel):
    name: str
    type: DbType


class MetaTable(BaseModel):
    name: str
    keys: dict[str, DbType]
    first_row_offset: int = 0
    last_row_offset: int = 0
    next_table_offset: int = 0
    prev_table_offset: int = 0

    def has_next(self):
        return self.next_table_offset > 0

    def has_prev(self):
        return self.prev_table_offset > 0


class MetaRow(BaseModel):
    data: dict
    next_row_offset: int = 0
    prev_row_offset: int = 0

    def has_next(self):
        return self.next_row_offset > 0

    def has_prev(self):
        return self.prev_row_offset > 0


class Table(BaseModel):
    name: str
    keys: dict[str, DbType]


class Row(BaseModel):
    data: dict


FilterValue = list | str
FilterPart = dict[str, FilterValue]
Filter = list[FilterPart] | FilterPart


class FilterRequest(BaseModel):
    filter_data: Filter
