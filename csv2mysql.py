"""
csv2mysql
~~~~~~~~~

Read csv file and store data on mySQL Database
"""
from argparse import ArgumentParser
from argparse import Namespace
from pathlib import Path
import logging

from MySQLdb import connect
from MySQLdb.connections import Connection
import yaml


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

def store_data(conn: Connection, config: dict):
    """Store data."""
    # Create DB
    database = config['mysql']['database']
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")

    # Read csv file and scheme

    # Create Table if not exist

    # Store data




def main():
    """Main Routine."""
    args = read_args()
    args.config = DIR / args.config
    config = read_config(args.config)
    conn = connect_database(config['mysql'])
    store_data(conn, config)

    conn.close()


if __name__ == '__main__':
    main()
