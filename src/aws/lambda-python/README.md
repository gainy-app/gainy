### Local environment

Locally we have a [simple router](../router), which is a single entrypoint for all requests to local lambdas.

Requests example:

```bash
# action
curl -XPOST "http://0.0.0.0:8082/hasuraAction" -H 'Content-Type: application/json' -d '{"action": {"name": "get_recommended_collections"}, "session_variables": {"x-hasura-user-id":"AO0OQyz0jyL5lNUpvKbpVdAPvlI3", "x-hasura-role": "user"}, "input": {"profile_id": "1"}}'

# trigger:
curl -XPOST "http://0.0.0.0:8082/hasuraTrigger" -H 'Content-Type: application/json' -d '{"event": {"session_variables": {"x-hasura-user-id":"AO0OQyz0jyL5lNUpvKbpVdAPvlI3", "x-hasura-role": "user"}, "op": "INSERT", "data": {"old": null, "new": {"email": "test8@example.com", "first_name": "fn", "legal_address": "legal_address", "gender": 0, "last_name": "ln", "created_at": "2021-10-07T10:17:05.429567+00:00", "id": 1, "avatar_url": "", "user_id": "AO0OQyz0jyL5lNUpvKbpVdAPvlI8"}}, "trace_context": {"trace_id": "b6e199ae300e13af", "span_id": "5b11f282cdfacac1"}}, "created_at": "2021-10-07T10:17:05.429567Z", "id": "cbbcde14-b09f-4b82-bab9-e75c4c863cd4", "delivery_info": {"max_retries": 0, "current_retry": 0}, "trigger": {"name": "on_user_created"}, "table": {"schema": "app", "name": "profiles"}}'
```
