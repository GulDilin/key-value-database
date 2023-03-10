import argparse
import random
import shlex
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from . import types
from .db import Database
from .util import (check_positive, execution_time, valid_filter,
                   valid_row_data, valid_table)


class CommandsEnum(types.StrEnum):
    CREATE_TABLE = 'create-table'
    CREATE_INDEX = 'create-index'
    LIST_TABLES = 'list-tables'
    SELECT = 'select'
    INSERT = 'insert'
    INSERT_AUTO = 'insert-auto'
    HELP = 'help'


@dataclass
class Parser:
    database: Database

    def __post_init__(self):
        self.COMMANDS_PARSERS = {
            CommandsEnum.SELECT: self.create_select_parser(),
            CommandsEnum.CREATE_TABLE: self.create_create_table_parser(),
            CommandsEnum.CREATE_INDEX: self.create_create_index_parser(),
            CommandsEnum.LIST_TABLES: self.create_list_tables_parser(),
            CommandsEnum.INSERT: self.create_insert_parser(),
            CommandsEnum.INSERT_AUTO: self.create_insert_auto_parser(),
        }
        self.COMMANDS: dict[str, Callable[[list[str]], None]] = {
            CommandsEnum.HELP: self.help_cmd,
            CommandsEnum.SELECT: self.select_command,
            CommandsEnum.INSERT: self.insert_command,
            CommandsEnum.INSERT_AUTO: self.insert_auto_command,
            CommandsEnum.LIST_TABLES: self.list_tables_command,
            CommandsEnum.CREATE_TABLE: self.create_table_command,
            CommandsEnum.CREATE_INDEX: self.create_index_command,
        }
        self.GENERATORS: dict[types.DbType, Callable[[], Any]] = {
            types.DbType.INT: self._gen_int,
            types.DbType.STR: self._gen_string,
        }

    @staticmethod
    def _gen_string():
        return str(uuid.uuid4())

    @staticmethod
    def _gen_int():
        return int(random.random() * 1000)

    def create_select_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(prog=CommandsEnum.SELECT, exit_on_error=False)
        parser.add_argument('--table', '-t', dest="table", type=str, required=True, help='Table name')
        parser.add_argument('--limit', '-l', dest="limit", type=check_positive, default=0, help='Rows limit')
        parser.add_argument(
            '--use-index', '-i',
            dest="use_index",
            action="store_true",
            default=False,
            help='Use indexes in select'
        )
        parser.add_argument('--all', dest="all", action="store_true", default=False, help='Do not pause select')
        parser.add_argument('--counter', dest="counter", action="store_true", default=False, help='Only count items')
        parser.add_argument(
            '--filter', '-f',
            dest="filter_",
            type=valid_filter,
            required=False,
            help=r'[{ key: val }, ... ] or { key: val, ... }'
        )
        return parser

    def create_create_table_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(prog=CommandsEnum.CREATE_TABLE, exit_on_error=False)
        parser.add_argument('table', type=valid_table, help=r'{ name, keys: { key: type } }')
        return parser

    def create_create_index_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(prog=CommandsEnum.CREATE_INDEX, exit_on_error=False)
        parser.add_argument('--table', '-t', dest="table", type=str, required=True, help='Table name')
        parser.add_argument('--key', '-k', dest="key", type=str, required=True, help='Table key')
        return parser

    def create_list_tables_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(prog=CommandsEnum.LIST_TABLES, exit_on_error=False)
        return parser

    def create_insert_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(prog=CommandsEnum.INSERT, exit_on_error=False)
        parser.add_argument('--table', '-t', dest="table", type=str, required=True, help='Table name')
        parser.add_argument(
            '--data', '-d',
            dest="data",
            type=valid_row_data,
            required=False,
            help=r'{ key: val, ... }'
        )
        return parser

    def create_insert_auto_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(prog=CommandsEnum.INSERT_AUTO, exit_on_error=False)
        parser.add_argument('--table', '-t', dest="table", type=str, required=True, help='Table name')
        parser.add_argument('--amount', '-a', dest="amount", type=check_positive, default=0, help='Rows amount')
        return parser

    def help_cmd(self, args: list[str]):
        for parser in self.COMMANDS_PARSERS.values():
            print(parser.format_help())
            print('-'*8 + '\n\n')

    @execution_time
    def list_tables_command(self, args_list: list[str]):
        for table in self.database.get_tables_iterator():
            print(table.dict())
        print('-'*8)

    def create_table_command(self, args_list: list[str]):
        try:
            args = self.COMMANDS_PARSERS[CommandsEnum.CREATE_TABLE].parse_intermixed_args(args_list)
        except SystemExit:
            return
        self.database.create_table(args.table)
        print('CREATED')

    @execution_time
    def create_index_command(self, args_list: list[str]):
        try:
            args = self.COMMANDS_PARSERS[CommandsEnum.CREATE_INDEX].parse_intermixed_args(args_list)
        except SystemExit:
            return
        self.database.create_table_index(args.table, args.key)
        print('INDEX CREATED')

    @execution_time
    def select_command(self, args_list: list[str]):
        try:
            args = self.COMMANDS_PARSERS[CommandsEnum.SELECT].parse_intermixed_args(args_list)
        except SystemExit:
            return

        i = 0
        if args.use_index:
            if len(args.filter_) == 0:
                raise ValueError('Filter cannot be empty for select using index')
            iterator = self.database.get_rows_iterator_use_indexes(args.table, args.filter_)
        else:
            iterator = self.database.get_rows_iterator(args.table, args.filter_)
        try:
            for row in iterator:
                if not args.counter:
                    print(row.dict())
                i += 1
                if i % 6 == 0 and not (args.all or args.counter):
                    input('--- Press to continue')
                if args.limit and i >= args.limit:
                    break
        except KeyboardInterrupt:
            pass
        print('-'*8 + f' select {i} items')

    @execution_time
    def insert_command(self, args_list: list[str]):
        try:
            args = self.COMMANDS_PARSERS[CommandsEnum.INSERT].parse_intermixed_args(args_list)
        except SystemExit:
            return
        self.database.insert_row(args.table, types.Row(data=args.data))
        print('INSERTED')

    @execution_time
    def insert_auto_command(self, args_list: list[str]):
        try:
            args = self.COMMANDS_PARSERS[CommandsEnum.INSERT_AUTO].parse_intermixed_args(args_list)
        except SystemExit:
            return
        table = self.database.get_table_by_name(args.table)
        i = 0
        try:
            for i in range(args.amount):
                data = {
                    key: self.GENERATORS[type_v]()
                    for key, type_v in table.keys.items()
                }
                self.database.insert_row(args.table, types.Row(data=data))
        except KeyboardInterrupt:
            pass
        print(f'INSERTED {i+1}')

    @staticmethod
    def parse_command(msg: str) -> tuple[str, list[str]]:
        splitted = msg.split(" ", 1)
        command = splitted[0].lower()
        args = []
        if len(splitted) > 1:
            args = shlex.split(splitted[1])
        return command, args

    def exec_cmd(self, msg: str):
        try:
            command, args = self.parse_command(msg)
            if command in self.COMMANDS:
                self.COMMANDS[command](args)
            else:
                print('Command not found. Type help to get info about available commands')
        except Exception as e:
            print(f'Got an error: {e}')
