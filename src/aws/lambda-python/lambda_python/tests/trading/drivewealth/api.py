import multiprocessing
from multiprocessing.dummy import Value

from gainy.utils import db_connect
from trading.drivewealth.api import DriveWealthApi
from trading.drivewealth.repository import DriveWealthRepository


def _get_token(*args):
    with db_connect() as db_conn:
        drivewealth_repository = DriveWealthRepository(db_conn)
        api = DriveWealthApi(drivewealth_repository)
    return api._get_token()


def test_get_token():
    threads_count = 5

    with multiprocessing.dummy.Pool(threads_count) as pool:
        tokens = pool.map(_get_token, range(threads_count))

    assert len(tokens) == threads_count
    for i in tokens:
        assert i == tokens[0]
