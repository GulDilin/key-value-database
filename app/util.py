import argparse
import json
import re

from pydantic.error_wrappers import ValidationError

from . import types


def print_pydantic_errors(exc: ValidationError):
    print('Error:')
    for err in exc.errors():
        print(err)


def convert_json(s: str) -> str:
    s = re.sub(r'(\s+)?:(\s+)?', ':', s)
    s = re.sub(r'((\s+)?),((\s+)?)', ',', s)
    s = re.sub(r'((\s+)?){((\s+)?)', '{', s)
    s = re.sub(r'((\s+)?)}((\s+)?)', '}', s)
    s = re.sub(r'(\w+)((\s+)?(\w+)?)', r'"\g<0>"', s)
    return s


def valid_json(s: str) -> dict:
    try:
        return json.loads(convert_json(s))
    except Exception as e:
        print(f'Incorrect JSON format: {s}')
        raise e


def valid_filter(s: str) -> types.Filter:
    return valid_json(s)


def valid_table(s: str) -> types.TableCreate:
    try:
        return types.TableCreate.parse_raw(convert_json(s))
    except ValidationError as e:
        print_pydantic_errors(e)
        raise e


def valid_row_data(s: str) -> dict:
    return valid_json(s)


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
