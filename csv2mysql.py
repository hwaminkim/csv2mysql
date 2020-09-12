"""
csv2mysql
~~~~~~~~~

Read csv file and store data on mySQL Database
"""
from argparse import ArgumentParser
from argparse import Namespace
from ast import literal_eval
from pathlib import Path
import csv
import datetime
import logging

import yaml
from MySQLdb import connect
from MySQLdb.connections import Connection
from MySQLdb.cursors import Cursor


FORMATTER = ('%(asctime)s : %(levelname)-8s : %(process)6d : '
             '%(name)s : %(module)s : %(message)s')
logging.basicConfig(format=FORMATTER, level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)

DIR = Path(__file__).parent.resolve()


class Csv2MysqlError(Exception):
    """Base Exception for this script."""


def read_config(path: Path) -> dict:
    """Read .yaml config file."""
    LOGGER.debug('Read config file: %r', path)
    with open(path, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            raise exc
    LOGGER.debug('Config: %r', config)
    return config


def read_args() -> Namespace:
    """Argument Parser for Phase 1."""
    parser = ArgumentParser(description="Invasive Aquatic Species Project")

    parser.add_argument(
        '-t', '--table', type=str, metavar='STRING', required=True,
        help="Target table name.")
    parser.add_argument(
        '-f', '--csv-file', type=str, metavar='STRING', required=True,
        help="Target csv file.")
    parser.add_argument(
        '-c', '--config', type=str, metavar='STRING', default='config.yaml',
        help="Configuration file for model.")
    parser.add_argument(
        '--clean-table', action='store_true',
        help="Configuration file for model.")
    res = parser.parse_args()
    LOGGER.info('Argument passed: %r', res)
    return res


def connect_database(config: dict) -> Connection:
    """Connect database."""
    return connect(host=config['host'], port=config['port'],
                   user=config['user'], passwd=config['password'])


def is_date(date_text: str) -> bool:
    """Validate date type string."""
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        return False
    return True


def data_type(token: str, max_len: int) -> str:
    """Parse token and return MYSQL type string."""
    def return_str() -> str:
        """Return string by its length."""
        if max_len < 254:
            return f'VARCHAR({max_len})'
        return 'TEXT({max_len})'

    def return_numerics(_eval_type: type) -> str:
        """Return numeric type."""
        if _eval_type in [True, False]:
            return 'BIT'
        if isinstance(_eval_type, int):
            return 'INT'
        if isinstance(_eval_type, float):
            return 'FLOAT'
        return return_str()

    token = token.strip()

    if len(token) == 0:
        # Empty token
        return return_str()
    if is_date(token):
        return 'DATE'
    try:
        eval_type = literal_eval(token)
    except (ValueError, SyntaxError):
        return return_str()

    if type(eval_type) in [int, float, bool]:
        return return_numerics(eval_type)

    LOGGER.debug('Unexpected token "%r", regard as string', token)
    return return_str()


def read_schema(path: Path) -> list:
    """
    Read schema from csv file

    return: [('name', str), ('id', int), ...]
    """
    res = []

    # Get Maximum length
    LOGGER.debug('Start to check maximum length string...')
    with path.open('r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        length = [0 for _ in next(csv_reader)]
        for line in csv_reader:
            length = [max(i, len(s)) for i, s in zip(length, line)]
    LOGGER.debug('Maximum length of column: %r', length)

    # Inspect type
    with path.open('r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        names = next(csv_reader)
        line = next(csv_reader)
        types = [data_type(l, max_len + 2) for l, max_len in zip(line, length)]

    LOGGER.debug('Inspect types: %r', list(zip(line, types)))
    LOGGER.info('CSV Schema: %r', res := list(zip(names, types)))
    return res



def store_data(cur: Cursor, config: dict, table: str, csv_file: Path):
    """Store data."""
    LOGGER.debug('Create Database if not exist')
    database = config['mysql']['database']
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    cur.execute(f"USE {database}")

    schema = read_schema(csv_file)

    types = ', '.join([f'{name} {type}' for name, type in schema])
    query_create_table = f"CREATE TABLE IF NOT EXISTS {table} ({types})"
    LOGGER.debug('Query> %s', query_create_table)
    cur.execute(query_create_table)

    LOGGER.debug('Check whether table %r is empty', table)
    cur.execute(f'SELECT COUNT(*) FROM {table}')
    if data := cur.fetchall()[0][0]:
        raise Csv2MysqlError(f'Table {table} is already having {data} rows')

    LOGGER.debug('Store data from %r', csv_file)
    query_store_data = (
        f"LOAD DATA LOCAL INFILE '{str(csv_file)}' INTO TABLE {table} FIELDS "
        f"TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' "
        "IGNORE 1 ROWS")
    LOGGER.debug('Query> %s', query_store_data)
    cur.execute(query_store_data)

    LOGGER.info('Finished to store data!')


def main():
    """Main Routine."""
    # Arg Parse
    args = read_args()

    # Read config
    config = read_config(args.config)

    # Store on DB
    conn = connect_database(config['mysql'])
    cur = conn.cursor()
    store_data(cur, config, args.table, Path(args.csv_file))
    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
