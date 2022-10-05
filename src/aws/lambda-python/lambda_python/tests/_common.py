PROFILE_ID = 1
PROFILE_ID2 = 2
USER_ID = 'AO0OQyz0jyL5lNUpvKbpVdAPvlI3'
USER_ID2 = 'AO0OQyz0jyL5lNUpvKbpVdAPvlI4'
PLAID_ACCESS_TOKEN = "access-sandbox-1b188d63-fea5-433e-aee9-30ba19cebda6"
PLAID_ITEM_ID = "Rbe7Mk36yxsZamjZLMGrtnpv3W6zx7uRwBdga"


def _get_session_variables(user_id: str):
    if user_id:
        return {
            "x-hasura-role": "user",
            "x-hasura-user-id": user_id,
        }

    return {"x-hasura-role": "admin"}


def get_action_event(name: str, input: dict, user_id: str):
    return {
        "action": {
            "name": name
        },
        "input": input,
        "session_variables": _get_session_variables(user_id),
    }


def get_trigger_event(name: str, op: str, data: dict, user_id: str):
    return {
        "trigger": {
            "name": name
        },
        "event": {
            "op": op,
            "data": data,
            "session_variables": {
                "x-hasura-role": "user",
                "x-hasura-user-id": user_id
            },
        },
    }