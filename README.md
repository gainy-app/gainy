# Gainy Backend

This repo contains all backend services for Gainy application and infrastructure.

## Local environment
### Prerequisites:
- Install [Docker](https://docs.docker.com/get-docker/)
- Configure AWS credentials. 

  You need AWS credentials with access to CodeArtifact to build the project. Copy these parameters to `.env.local` and fill with appropriate values.
  ```bash
  AWS_ACCESS_KEY_ID=
  AWS_SECRET_ACCESS_KEY=
  AWS_DEFAULT_REGION=us-east-1
  ```

### Running
```bash
make configure up
```
After the project is running:
- check [http://0.0.0.0:8080/](http://0.0.0.0:8080/) for Hasura GraphQL console 
- check [http://0.0.0.0:5001/](http://0.0.0.0:5001/) for Meltano Airflow dashboard 

### Development
Useful commands:
- `make hasura-console` will run Hasura development console at [http://0.0.0.0:9695/](http://0.0.0.0:9695/). It's needed to persist schema and configuration changes and create migrations.
- `make style-fix` to run style-check and change files in-place.
- `make start-realtime` to run realtime listener.

## Key parts of the app
1. ### [Data pipelines](src/meltano)
   We use [Meltano](https://meltano.com/) ELT framework to pull data from our data providers and transform it in a way the mobile app can cosume it. 
   The whole pipeline consists of 3 steps:
   1. Extractor 
   
      We have custom written taps for the `extract` stage:
      - [tap-eodhistoricaldata](https://github.com/gainy-app/gainy-docker-images/tree/main/docker/meltano/tap-eodhistoricaldata) for  eodhistoricaldata.com
      - [tap-polygon](https://github.com/gainy-app/gainy-docker-images/tree/main/docker/meltano/tap-polygon) for polygon.com
   2. Loader

      We use standard [target-postgres](https://github.com/transferwise/pipelinewise-target-postgres) to load data into our Postgres instance as is. Basically it ends up in database as one giant json blob

   3. Transformer
   
   At the end we use [dbt](https://www.getdbt.com/) transformer to normalize loaded data into [models](src/meltano/meltano/transform/models).

2. ### [GraphQL API](src/hasura)
   Our API is built using [Hasura](https://hasura.io/) GraphQL server. It connects to our Postgres instance and generates GraphQL API on top of it. 

3. ### [Lambda functions](./src/aws/lambda-python)
   All the business logics we have is written in the form of lambda functions. They are triggered directly by Hasura, either via specific requests (actions), or when some data changes (triggers).

3. ### [Websockets](./src/websockets)
   All the realtime data we collect is done in this module.

4. ### [Terraform](./terraform)
   All our infrastructure is managed by terraform.
   
   We have two environments: `production` and `test`:
   - `production` environment deployment is triggered upon a push to the `main` repository.
   - `test` environment deployment is triggered manually [here](https://github.com/gainy-app/gainy/actions/workflows/terraform.yml).

## Key integrations
### Portfolio
Flow:
1. Plaid auth flow: app requests `link token`, exchanges it via a client library for a `public token`, sends it to backend and backend exchanges it for an eternal `access token`.
2. When the `access token` is saved, a trigger `on_plaid_access_token_created` is executed and pulls  data from plaid.
3. dbt then calculates chart and gains [here](src/meltano/meltano/transform/models/portfolio)

## Tests
Execute this command to run tests:
```bash
make test
```
Coverage:
- dbt: all the exposed models
- hasura: chart, collections, portfolio, interests, categories
- lambda: recommended collections
