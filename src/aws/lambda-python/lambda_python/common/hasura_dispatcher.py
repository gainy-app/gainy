import json
import traceback
from abc import ABC, abstractmethod
from typing import List
import psycopg2
from psycopg2 import sql
from gainy.utils import db_connect
from common.hasura_exception import HasuraActionException
from common.hasura_function import HasuraAction, HasuraTrigger
from common.hasura_response import base_response
from gainy.utils import get_logger

logger = get_logger(__name__)


class HasuraDispatcher(ABC):

    def __init__(self, functions, is_gateway_proxy=True):
        self.functions = functions
        self.is_gateway_proxy = is_gateway_proxy

    def handle(self, event, context=None):
        headers = event['headers'] if 'headers' in event else {}
        request = self.extract_request(event)

        with db_connect() as db_conn:
            try:
                response = self.apply(db_conn, request, headers)

                return self.format_response(200, response)
            except HasuraActionException as he:
                logger.warning(f"{he.http_code} {he.message}. event: {event}")

                return self.format_response(he.http_code, {
                    "message": he.message,
                    "code": he.http_code
                })
            except Exception as e:
                logger.exception("Event: %s", event)

                return self.format_response(500, {
                    "message": str(e),
                    "code": 500
                })

    @abstractmethod
    def apply(self, dn_conn, request, headers):
        pass

    def choose_function_by_name(self, function_name):
        filtered_actions = list(
            filter(lambda function: function.is_applicable(function_name),
                   self.functions))
        if len(filtered_actions) != 1:
            raise HasuraActionException(
                400, f"`{function_name}` is not a valid action or trigger")

        return filtered_actions[0]

    def extract_request(self, event):
        if self.is_gateway_proxy:
            return json.loads(event["body"])
        else:
            return json.loads(event) if isinstance(event, str) else event

    def format_response(self, http_code, response_body):
        return base_response(
            http_code,
            response_body) if self.is_gateway_proxy else response_body

    def get_profile_id(self, db_conn, session_variables):
        hasura_role = session_variables["x-hasura-role"]

        if hasura_role in ["admin", "anonymous"]:
            return None

        hasura_user_id = session_variables.get("x-hasura-user-id")
        if hasura_user_id is None:
            return None

        with db_conn.cursor() as cursor:
            cursor.execute(f"SELECT id FROM app.profiles WHERE user_id = %s",
                           (hasura_user_id, ))

            user = cursor.fetchone()
            if user is None:
                return None

        return user[0]

    def check_authorization(self, db_conn, allowed_profile_ids,
                            session_variables):
        try:
            hasura_role = session_variables["x-hasura-role"]

            if "admin" == hasura_role:
                return

            session_profile_id = self.get_profile_id(db_conn,
                                                     session_variables)
        except Exception:
            raise HasuraActionException(
                401, f"Unauthorized access to profile `{allowed_profile_ids}`")

        if not isinstance(allowed_profile_ids, list):
            allowed_profile_ids = [allowed_profile_ids]

        if session_profile_id not in allowed_profile_ids:
            raise HasuraActionException(
                401, f"Unauthorized access to profile `{allowed_profile_ids}`")


class HasuraActionDispatcher(HasuraDispatcher):

    def __init__(self,
                 actions: List[HasuraAction],
                 is_gateway_proxy: bool = True):
        super().__init__(actions, is_gateway_proxy)

    def apply(self, db_conn, request, headers):
        action = self.choose_function_by_name(request["action"]["name"])

        input_params = request["input"]

        # public endpoints won't be tied to profile
        if action.profile_id_param is not None:
            profile_id = action.get_profile_id(input_params)
            self.check_authorization(db_conn, profile_id,
                                     request["session_variables"])
        else:
            action.profile_id = self.get_profile_id(
                db_conn, request["session_variables"])

        return action.apply(db_conn, input_params, headers)


class HasuraTriggerDispatcher(HasuraDispatcher):

    def __init__(self,
                 triggers: List[HasuraTrigger],
                 is_gateway_proxy: bool = True):
        super().__init__(triggers, is_gateway_proxy)

    def apply(self, db_conn, request, headers):
        trigger = self.choose_function_by_name(request["trigger"]["name"])

        op = request["event"]["op"]
        data = request["event"]["data"]

        allowed_profile_ids = trigger.get_allowed_profile_ids(op, data)

        session_variables = request["event"]["session_variables"]
        # TODO if trigger is called via db operation directly (not via hasura) - it won't have hasura role
        if session_variables is not None:
            self.check_authorization(db_conn, allowed_profile_ids,
                                     session_variables)

        return trigger.apply(db_conn, op, data)
