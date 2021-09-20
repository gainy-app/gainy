### Local environment

```bash
docker build -t lambda-python .
docker run -p 9000:8080 --env-file ../../../.env -v $(pwd)/lambda_python:/var/task lambda-python "hasura_action.handle"
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"headers": {"X-Hasura-User-ID":"USER_ID"}}'
```