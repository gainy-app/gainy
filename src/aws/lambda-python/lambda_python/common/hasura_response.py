from typing import Optional

import datetime

import json

from gainy.utils import DATETIME_ISO8601_FORMAT_TZ


def base_response(status_code, body=None):
    return {
        "isBase64Encoded": False,
        "statusCode": status_code,
        "headers": {},
        "body": json.dumps(body, default=str),
    }


def error_response(status_code, message, payload={}):
    payload = payload.copy()
    payload['message'] = message
    return base_response(status_code, payload)


def format_datetime(d: Optional[datetime.datetime]):
    return d.strftime(DATETIME_ISO8601_FORMAT_TZ) if d else None
