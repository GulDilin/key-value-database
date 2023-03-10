import argparse
import json
import random
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from pydantic.error_wrappers import ValidationError

from . import types
from .db import Database


def print_pydantic_errors(exc: ValidationError):
    for err in exc.errors():
        print(err['msg'])


def valid_filter(s: str) -> types.Filter:
    try:
        return json.loads(s)
    except Exception as e:
        print('Incorrect JSON format')
        raise e


def valid_table(s: str) -> types.Table:
    try:
        return types.Table.parse_raw(s)
    except ValidationError as e:
        print_pydantic_errors(e)
        raise e


def valid_row_data(s: str) -> dict:
    try:
        return json.loads(s)
    except Exception as e:
        print('Incorrect JSON format')
        raise e


def check_positive(value):
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
    return ivalue


def execution_time(func):
    import time

    def wrapper(*args, **kwargs):
        start = time.time()
        return_value = func(*args, **kwargs)
        end = time.time()
        print('Execution time: {} s.'.format(end-start))
        return return_value
    return wrapper


class CommandsEnum(types.StrEnum):
    CREATE_TABLE = 'create-table'
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
        return random.random()

    def create_select_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(prog=CommandsEnum.SELECT, exit_on_error=False)
        parser.add_argument('--table', '-t', dest="table", type=str, required=True, help='Table name')
        parser.add_argument('--limit', '-l', dest="limit", type=check_positive, default=0, help='Rows limit')
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
            print('-'*8)

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
    def select_command(self, args_list: list[str]):
        try:
            args = self.COMMANDS_PARSERS[CommandsEnum.SELECT].parse_intermixed_args(args_list)
        except SystemExit:
            return

        i = 0
        try:
            for row in self.database.get_rows_iterator(args.table, args.filter_):
                if not args.counter:
                    print(row.dict())
                i += 1
                if i % 6 == 0 and not args.all:
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
        for i in range(args.amount):
            data = {
                key: self.GENERATORS[type_v]()
                for key, type_v in table.keys.items()
            }
            self.database.insert_row(args.table, types.Row(data=data))
        print('INSERTED')

    @staticmethod
    def parse_command(msg: str) -> tuple[str, list[str]]:
        splitted = msg.split(" ", 1)
        command = splitted[0].lower()
        args = []
        if len(splitted) > 1:
            args = splitted[1].split(';')
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
