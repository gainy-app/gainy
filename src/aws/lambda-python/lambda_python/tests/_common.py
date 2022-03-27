PROFILE_ID = 1
USER_ID = 'AO0OQyz0jyL5lNUpvKbpVdAPvlI3'

def _get_session_variables(user_id: str):
    if user_id:
        return {
            "x-hasura-role": "user",
            "x-hasura-user-id": user_id,
        }

    return {"x-hasura-role": "admin"}

def get_action_event(name: str, input: dict, user_id: str):
    return {
        "action": {"name": name},
        "input": input,
        "session_variables": _get_session_variables(user_id),
    }

def get_trigger_event(name: str, op: str, data: dict, user_id: str):
    return {
        "trigger": {"name": name},
        "event": {
            "op": op,
            "data": data,
            "session_variables": None,
        },
    }