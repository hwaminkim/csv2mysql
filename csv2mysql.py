"""
csv2mysql
~~~~~~~~~

Read csv file and store data on mySQL Database
"""
from pathlib import Path
import logging

import yaml


FORMATTER = ('%(asctime)s : %(levelname)-8s : %(process)6d : '
             '%(name)s : %(module)s : %(message)s')
logging.basicConfig(format=FORMATTER, level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)

DIR = Path(__file__).parent.resolve()
CONFIG = DIR / 'config.yaml'


def read_config(path: Path) -> dict:
    """Read .yaml config file."""
    with open(path, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            raise exc
    return config


def main():
    """Main Routine."""
    config = read_config(CONFIG)
    print(config)


if __name__ == '__main__':
    main()
