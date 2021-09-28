# Gainy Backend

This repo contains all backend services for Gainy application and infrastructure.

## Local environment
### Setting up
```bash
cp .env.dist .env # ask for the right env config from the team
make update-quick # sometimes first run fails - to be investigated
```

### Running
- check [http://localhost:8081/](http://localhost:8081/) for Firebase Auth demo page 
- check [http://localhost:8080/](http://localhost:8080/) for Hasura GraphQL console 
- run `make hasura-console` and check [http://localhost:9695/](http://localhost:9695/) for Hasura development console 
- check [http://localhost:5000/](http://localhost:5000/) for Meltano ETL console 

## Key parts of the app
1. ### Gainy Fetch - an ELT pipeline
   Gainy Fetch is a data pipeline built on top of [Meltano](https://meltano.com/) ETL framework. 
   The whole pipeline consists of 3 steps:
   1. Extractor 
   We have a custom written [tap-eodhistoricaldata](https://github.com/gainy-app/gainy/tree/main/src/gainy-fetch/tap-eodhistoricaldata) to pull data from eodhistoricaldata.com

   2. Loader
   We use standard [target-postgres](https://github.com/transferwise/pipelinewise-target-postgres) to load data into our PostgreSQL instance as is. Basically it ends up in database as one giant json blob

   3. Transformer (not configured to be automatially run
   At the end we use [dbt](https://www.getdbt.com/) transformer to normalize loaded data into [models](https://github.com/gainy-app/gainy/tree/main/src/gainy-fetch/meltano/transform/models).

2. ### Gainy API
   [Gainy API](https://github.com/gainy-app/gainy-etl/tree/main/src/hasura) is built using [Hasura](https://hasura.io/) GraphQL server. It connects to our PostgeSQL instance and generates [GraphQL API](https://gainy-dev.herokuapp.com/v1/graphql) on top of it. 

3. ### Terraform
   All our infrastructure is managed by terraform and can be found in [terraform](https://github.com/gainy-app/gainy/tree/main/terraform) folder.
   
   We have two environments: `production` and `test`.
   `production` environment deployment is triggered upon a push to the `main` repository.
   `test` environment deployment is triggered manually [here](https://github.com/gainy-app/gainy/actions/workflows/deploy_to_test.yml).
