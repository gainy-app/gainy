from typing import Optional

import datetime

import json


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
    return d.strftime("%Y-%m-%dT%H:%M:%S%z") if d else None
