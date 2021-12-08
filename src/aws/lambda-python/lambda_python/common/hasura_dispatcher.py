import json
import traceback
from abc import ABC, abstractmethod
from typing import List

import psycopg2

from common.hasura_exception import HasuraActionException
from common.hasura_function import HasuraAction, HasuraTrigger
from common.hasura_response import base_response


class HasuraDispatcher(ABC):
    def __init__(self, db_conn_string, functions, is_gateway_proxy=True):
        self.db_conn_string = db_conn_string
        self.functions = functions
        self.is_gateway_proxy = is_gateway_proxy

    def handle(self, event, context):
        print(event)
        request = self.extract_request(event)

        with psycopg2.connect(self.db_conn_string) as db_conn:
            try:
                response = self.apply(db_conn, request)

                return self.format_response(200, response)
            except HasuraActionException as he:
                print(f"event: {event}")
                traceback.print_exc()
                return self.format_response(he.http_code, {
                    "message": he.message,
                    "code": he.http_code
                })
            except Exception as e:
                print(f"event: {event}")
                traceback.print_exc()
                return self.format_response(500, {
                    "message": str(e),
                    "code": 500
                })

    @abstractmethod
    def apply(self, dn_conn, request):
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

    def check_authorization(self, db_conn, profile_id, session_variables):
        try:
            hasura_role = session_variables["x-hasura-role"]

            if "admin" == hasura_role:
                return

            with db_conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT user_id FROM app.profiles WHERE id = %s",
                    (profile_id, ))

                user = cursor.fetchone()
                if user is None:
                    raise Exception('User not found')

            user_id = user[0]
            hasura_user_id = session_variables["x-hasura-user-id"]
        except Exception:
            traceback.print_exc()
            raise HasuraActionException(
                401, f"Unauthorized access to profile `{profile_id}`")

        if user_id != hasura_user_id:
            raise HasuraActionException(
                401, f"Unauthorized access to profile `{profile_id}`")


class HasuraActionDispatcher(HasuraDispatcher):
    def __init__(self,
                 db_conn_string: str,
                 actions: List[HasuraAction],
                 is_gateway_proxy: bool = True):
        super().__init__(db_conn_string, actions, is_gateway_proxy)

    def apply(self, db_conn, request):
        action = self.choose_function_by_name(request["action"]["name"])

        input_params = request["input"]

        # public endpoints won't be tied to profile
        if action.profile_id_param is not None:
            profile_id = action.get_profile_id(input_params)
            self.check_authorization(db_conn, action,
                                     request["session_variables"])

        return action.apply(db_conn, input_params)


class HasuraTriggerDispatcher(HasuraDispatcher):
    def __init__(self,
                 db_conn_string: str,
                 triggers: List[HasuraTrigger],
                 is_gateway_proxy: bool = True):
        super().__init__(db_conn_string, triggers, is_gateway_proxy)

    def apply(self, db_conn, request):
        trigger = self.choose_function_by_name(request["trigger"]["name"])

        op = request["event"]["op"]
        data = request["event"]["data"]

        profile_id = trigger.get_profile_id(op, data)

        session_variables = request["event"]["session_variables"]
        # TODO if trigger is called via db operation directly (not via hasura) - it won't have hasura role
        if session_variables is not None:
            self.check_authorization(db_conn, profile_id, session_variables)

        return trigger.apply(db_conn, op, data)
