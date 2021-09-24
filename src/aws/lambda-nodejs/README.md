### Local environment

```bash
docker build -t lambda-nodejs .
docker run -p 9010:8080 --env-file ../../../.env -v $(pwd):/var/task --rm --name gainy-lambda-nodejs lambda-nodejs index.fetchLivePrices
curl -XPOST "http://localhost:9010/2015-03-31/functions/function/invocations"
```
