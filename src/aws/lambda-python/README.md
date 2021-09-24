### Local environment

```bash
docker build -t lambda-python .
docker run -p 9010:8080 --env-file ../../../.env -v $(pwd)/lambda_python:/var/task --rm --name gainy-lambda-python lambda-python "get_recommended_collections.handle"
curl -XPOST "http://localhost:9010/2015-03-31/functions/function/invocations" -d '{"session_variables":{"x-hasura-user-id":"AO0OQyz0jyL5lNUpvKbpVdAPvlI3"}}'
```

### Remote environment

```bash
curl -XPOST "https://6jif9avnjb.execute-api.us-east-1.amazonaws.com/serverless_lambda_stage_production/setUserCategories" -H 'Content-Type: application/json' -d '{"event":{"session_variables":{"x-hasura-user-id":"AO0OQyz0jyL5lNUpvKbpVdAPvlI3"}}}'
```