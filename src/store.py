import sqlite3

from structs import TickerType, DocType
from logging import getLogger

log = getLogger('STORE')

DB_PATH = '/tmp/db'


_store_connection: sqlite3.Connection | None = None
_cursor: sqlite3.Cursor | None = None


def init_store():
    open_store_conn()
    create_downloaded_info_table()


def open_store_conn():
    global _store_connection
    global _cursor
    if _store_connection is not None:
        close_store_conn()
    _store_connection = sqlite3.connect(DB_PATH)
    _cursor = _store_connection.cursor()


def close_store_conn() -> None:
    global _store_connection
    if _store_connection is None:
        print('store connection already closed')
    _store_connection.close()
    _store_connection = None


def create_downloaded_info_table():
    _cursor.execute("""
        CREATE TABLE IF NOT EXISTS DownloadedDocs (
            id INTEGER PRIMARY KEY, 
            ticker TEXT NOT NULL, 
            doc_type TEXT NOT NULL,
            dir_path TEXT NOT NULL,
            updated_at timestamp DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ticker, doc_type) ON CONFLICT REPLACE
        )
    """)
    _store_connection.commit()


def save_info(ticker: TickerType, doc_type: DocType, save_dir: str) -> None:
    _cursor.execute('INSERT INTO DownloadedDocs (ticker, doc_type, dir_path) VALUES (?, ?, ?)',
                    (ticker, doc_type, save_dir))
    _store_connection.commit()


def get_info(ticker: TickerType, doc_type: DocType | None = None) -> list[tuple[TickerType, DocType, str, str]]:
    if doc_type is None:
        ql = 'SELECT * FROM DownloadedDocs WHERE ticker =?'
        params = (ticker,)
    else:
        ql = 'SELECT * FROM DownloadedDocs WHERE ticker =? AND doc_type =?'
        params = (ticker, doc_type)
    _cursor.execute(ql, params)
    return _cursor.fetchall()
