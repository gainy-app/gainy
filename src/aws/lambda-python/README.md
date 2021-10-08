### Local environment

```bash
docker build -t lambda-python .
docker run -p 9010:8080 --env-file ../../../.env -v $(pwd)/lambda_python:/var/task --rm --name gainy-lambda-python lambda-python "hasura_handler.handle_action"
curl -XPOST "http://localhost:9010/2015-03-31/functions/function/invocations" -d '{"action": {"name": "get_recommended_collections"}, "session_variables": {"x-hasura-user-id":"AO0OQyz0jyL5lNUpvKbpVdAPvlI3"}, "input": {"profile_id": "4"}}'

# trigger:
curl -XPOST "http://localhost:9010/2015-03-31/functions/function/invocations" -d '{"event": {"session_variables": {"x-hasura-role": "admin"}, "op": "INSERT", "data": {"old": null, "new": {"email": "test8@example.com", "first_name": "fn", "legal_address": "legal_address", "gender": 0, "last_name": "ln", "created_at": "2021-10-07T10:17:05.429567+00:00", "id": 103, "avatar_url": "", "user_id": "AO0OQyz0jyL5lNUpvKbpVdAPvlI8"}}, "trace_context": {"trace_id": "b6e199ae300e13af", "span_id": "5b11f282cdfacac1"}}, "created_at": "2021-10-07T10:17:05.429567Z", "id": "cbbcde14-b09f-4b82-bab9-e75c4c863cd4", "delivery_info": {"max_retries": 0, "current_retry": 0}, "trigger": {"name": "on_user_created"}, "table": {"schema": "app", "name": "profiles"}}'
```

### Remote environment

```bash
curl -XPOST "https://6jif9avnjb.execute-api.us-east-1.amazonaws.com/serverless_lambda_stage_production/setUserCategories" -H 'Content-Type: application/json' -d '{"action": {"name": "get_recommended_collections"}, "session_variables": {"x-hasura-user-id":"AO0OQyz0jyL5lNUpvKbpVdAPvlI3"}, "input": {"profile_id": "4"}}'
```