"""
csv2mysql
~~~~~~~~~

Read csv file and store data on mySQL Database
"""
from ast import literal_eval
import datetime
from argparse import ArgumentParser
from argparse import Namespace
import csv
from pathlib import Path
import logging

import yaml
from MySQLdb import connect
from MySQLdb.connections import Connection


FORMATTER = ('%(asctime)s : %(levelname)-8s : %(process)6d : '
             '%(name)s : %(module)s : %(message)s')
logging.basicConfig(format=FORMATTER, level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)

DIR = Path(__file__).parent.resolve()


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



def store_data(conn: Connection, config: dict, table: str, csv_file: Path):
    """Store data."""
    # Create DB
    database = config['mysql']['database']
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    cur.execute(f"USE {database}")

    # Read csv file and scheme
    schema = read_schema(csv_file)

    # Create Table if not exist
    types = ', '.join([f'{name} {type}' for name, type in schema])
    query_create_table = f"CREATE TABLE IF NOT EXISTS {table} ({types})"
    LOGGER.debug('Query> %s', query_create_table)
    cur.execute(query_create_table)

    # Store data
    LOGGER.debug('Start to store data from %r', csv_file)
    query_store_data = (
        f"LOAD DATA LOCAL INFILE '{str(csv_file)}' INTO TABLE {table} FIELDS "
        f"TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' "
        "IGNORE 1 ROWS")
    LOGGER.debug('Query> %s', query_store_data)
    cur.execute(query_store_data)

    LOGGER.info('Finished to store data!')


def main():
    """Main Routine."""
    args = read_args()

    # Read config
    config = read_config(args.config)

    conn = connect_database(config['mysql'])
    store_data(conn, config, args.table, Path(args.csv_file))
    conn.close()


if __name__ == '__main__':
    main()
