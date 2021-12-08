from flask import Flask, Response, request
import requests

excluded_headers = [
    'content-encoding', 'content-length', 'transfer-encoding', 'connection'
]

app = Flask(__name__)


@app.route('/hasuraAction', methods=['POST'])
def proxy_python_hasura_action():
    resp = requests.post(
        "http://lambda-python-action:8080/2015-03-31/functions/function/invocations",
        json=request.get_json(),
        headers=request.headers)

    headers = [(name, value) for (name, value) in resp.raw.headers.items()
               if name.lower() not in excluded_headers]

    return Response(resp.content, resp.status_code, headers)


@app.route('/hasuraTrigger', methods=['POST'])
def proxy_python_hasura_trigger():
    resp = requests.post(
        "http://lambda-python-trigger:8080/2015-03-31/functions/function/invocations",
        json=request.get_json(),
        headers=request.headers)

    headers = [(name, value) for (name, value) in resp.raw.headers.items()
               if name.lower() not in excluded_headers]

    return Response(resp.content, resp.status_code, headers)
