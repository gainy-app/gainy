from flask import Flask, Response, request
import requests
import logging
import threading
import os

LAMBDA_PYTHON_ACTION_HOST = os.getenv('LAMBDA_PYTHON_ACTION_HOST')
LAMBDA_PYTHON_TRIGGER_HOST = os.getenv('LAMBDA_PYTHON_TRIGGER_HOST')

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

lock = threading.Lock()

excluded_headers = [
    'content-encoding', 'content-length', 'transfer-encoding', 'connection'
]

app = Flask(__name__)


@app.route('/hasuraAction', methods=['POST'])
def proxy_python_hasura_action():
    data = request.get_json()
    action_name = data['action']['name']

    try:
        with lock:
            resp = requests.post(
                "http://%s:8080/2015-03-31/functions/function/invocations" %
                (LAMBDA_PYTHON_ACTION_HOST),
                json=request.get_json(),
                headers=request.headers)
    except Exception as e:
        logger.error('%s 500: %s', action_name, e)
        raise e

    logger.info('%s %d: %s', action_name, resp.status_code, resp.content)

    headers = [(name, value) for (name, value) in resp.raw.headers.items()
               if name.lower() not in excluded_headers]

    return Response(resp.content, resp.status_code, headers)


@app.route('/hasuraTrigger', methods=['POST'])
def proxy_python_hasura_trigger():
    data = request.get_json()
    trigger_name = data['trigger']['name']

    try:
        with lock:
            resp = requests.post(
                "http://%s:8080/2015-03-31/functions/function/invocations" %
                (LAMBDA_PYTHON_TRIGGER_HOST),
                json=data,
                headers=request.headers)
    except Exception as e:
        logger.error('%s 500: %s', trigger_name, e)
        raise e

    logger.info('%s %d: %s', trigger_name, resp.status_code, resp.content)

    headers = [(name, value) for (name, value) in resp.raw.headers.items()
               if name.lower() not in excluded_headers]

    return Response(resp.content, resp.status_code, headers)
