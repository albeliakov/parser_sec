import argparse
import logging
import logging.handlers
import sys

from main import main


LOG_PATH = '/tmp/log'


def init_logging() -> None:
    logging.basicConfig(
        level='INFO',
        format='%(asctime)-17s  %(name)-8s  %(levelname)-8s  %(message)s',
        datefmt='%H:%M:%S %d.%m.%y',
        handlers=[
            logging.handlers.RotatingFileHandler(LOG_PATH, maxBytes=52428800, backupCount=2),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.getLogger('httpx').setLevel(logging.WARNING)


def cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('ticker', type=str, help='Ticker for which to run the pipeline')
    parser.add_argument('doctype', choices=['10-K', '10-Q', '8-K'], help='Document type')
    parser.add_argument('--save-dir', type=str, default='.', help='Directory to save the documents')
    return parser


if __name__ == '__main__':
    init_logging()
    args = cli().parse_args()
    logging.info('---------- Starting pipeline ----------')
    main(args)
