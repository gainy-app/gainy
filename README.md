# Gainy Backend

This repo contains all backend services for Gainy application and infrastructure.

# Gainy Fetch - an ELT pipeline

Gainy Fetch is a data pipeline built on top of [Meltano](https://meltano.com/) ETL framework.

The whole pipeline consists of 3 steps:

1. Extractor 
We have a custom written [tap-eodhistoricaldata](https://github.com/gainy-app/gainy-etl/tree/main/src/gainy-fetch/tap-eodhistoricaldata) to pull data from eodhistoricaldata.com

2. Loader
We use standard [target-postgres](https://github.com/transferwise/pipelinewise-target-postgres) to load data into our PostgreSQL instance as is. Basically it ends up in database as one giant json blob

3. Transfrormer (not configured to be automatially run
At the end we use [dbt](https://www.getdbt.com/) transformer to normalize loaded data into [models](https://github.com/gainy-app/gainy-etl/tree/main/src/gainy-fetch/meltano/transform/models).


# Gainy API

[Gainy API](https://github.com/gainy-app/gainy-etl/tree/main/src/hasura) is built using [Hasura](https://hasura.io/) GraphQL server. It connects to our PostgeSQL instance and generates [GraphQL API](https://gainy-dev.herokuapp.com/v1/graphql) on top of it. 


# Terraform

All our infrastructure is managed by terraform and can be found in [terraform](https://github.com/gainy-app/gainy-etl/tree/main/terraform) folder.
