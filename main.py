import argparse

from app.db import Database
from app.parser import Parser


def main():
    parser = argparse.ArgumentParser(prog="select")
    parser.add_argument('--db-file', '-d', dest="db_file", required=True, help='Database filename or path')

    args = parser.parse_args()
    database = Database(db_file=args.db_file)
    parser = Parser(database=database)
    print('Init connection')
    while True:
        try:
            msg = input('$> ')
            parser.exec_cmd(msg)
        except KeyboardInterrupt:
            database.indexer.save()
            break


if __name__ == "__main__":
    main()
