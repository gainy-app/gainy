### Local environment

```bash
docker build -t lambda-python .
docker run -p 9010:8080 --env-file ../../../.env -v $(pwd)/lambda_python:/var/task --rm --name gainy-lambda-python lambda-python "hasura_handler.handle_action"
curl -XPOST "http://localhost:9010/2015-03-31/functions/function/invocations" -d '{"action": {"name": "get_recommended_collections"}, "session_variables": {"x-hasura-user-id":"AO0OQyz0jyL5lNUpvKbpVdAPvlI3"}, "input": {"profile_id": "4"}}'
```

### Remote environment

```bash
curl -XPOST "https://6jif9avnjb.execute-api.us-east-1.amazonaws.com/serverless_lambda_stage_production/setUserCategories" -H 'Content-Type: application/json' -d '{"action": {"name": "get_recommended_collections"}, "session_variables": {"x-hasura-user-id":"AO0OQyz0jyL5lNUpvKbpVdAPvlI3"}, "input": {"profile_id": "4"}}'
```