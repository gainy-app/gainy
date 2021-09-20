import os
import traceback

import psycopg2

from common.hasura_exception import HasuraActionException
from common.hasura_response import base_response
from recommendation import recommendations

API_GATEWAY_PROXY_INTEGRATION = os.environ.get("AWS_LAMBDA_API_GATEWAY_PROXY_INTEGRATION", "True") == "True"


class Action:

    def __init__(self, func, profile_id_param=None):
        self.func = func
        self.profile_id_param = profile_id_param


action_registry = {
    "recommendProfileCollections": Action(recommendations.recommend_profile_collections, "profile_id")
}


def handle(event, context):
    print(event)
    request_body = extract_request_body(event)

    input_params = request_body["input"]
    session_variables = request_body["session_variables"]
    action = request_body["action"]

    with psycopg2.connect(get_conn_string_from_env()) as db_conn:
        try:
            action = action_registry[action["name"]]

            check_authorization(db_conn, action, input_params, session_variables)

            response_body = action.func(db_conn, input_params, session_variables)
            return format_response(200, response_body)
        except HasuraActionException as he:
            traceback.print_exc()
            return format_response(he.http_code, {"message": he.message, "code": he.http_code})
        except Exception as e:
            traceback.print_exc()
            return format_response(500, {"message": str(e), "code": 500})


def extract_request_body(event):
    return event["body"]["event"] if API_GATEWAY_PROXY_INTEGRATION else event


def format_response(http_code, response_body):
    return base_response(http_code, response_body) if API_GATEWAY_PROXY_INTEGRATION else response_body


def get_conn_string_from_env():
    host = os.environ['pg_host']
    port = os.environ['pg_port']
    dbname = os.environ['pg_dbname']
    username = os.environ['pg_username']
    password = os.environ['pg_password']

    return f"postgresql://{username}:{password}@{host}:{port}/{dbname}"


def check_authorization(db_conn, action, input_params, session_variables):
    if action.profile_id_param:
        profile_id = input_params.get(action.profile_id_param, None)
        if not profile_id:
            raise HasuraActionException(400, "Profile id is not provided")

        hasura_role = session_variables.get("x-hasura-role", None)
        if not hasura_role:
            raise HasuraActionException(401, "Hasura role is not provided")

        if "admin" != hasura_role:
            cursor = db_conn.cursor()
            cursor.execute(f"SELECT user_id FROM app.profiles WHERE id = {profile_id}")
            rows = list(cursor.fetchall())
            if len(rows) == 0:
                raise HasuraActionException(400, f"Incorrect profile id `{profile_id}`")

            user_id = rows[0][0]
            hasura_user_id = session_variables.get("x-hasura-user-id", None)
            if not hasura_user_id:
                raise HasuraActionException(401, "Hasura user id is not provided")

            if user_id != hasura_user_id:
                raise HasuraActionException(401, f"Unauthorized access to profile id `{profile_id}`")
