# Prerequisites

1. Install docker and docker-compose
2. Grab api token for eodhistoricaldata

# Running locally

1. Edit .env file and replace XXX with eodhistoricaldata api token

```bash
docker-compose build
docker-compose up
```

# Executing commands

After this you can visit http://localhost:5000 to see meltano console

To trigger scheduled ETL pipeline via console open separate terminal window and execute

```bash
docker-compose exec meltano meltano schedule run eodhistoricaldata-to-postgres-0 --transform=run
```

To connect to local postgresql database

```
psql -h localhost -U postgres # and enter postgrespassword
```

# Running against production (assuming you have access to heroku)

```bash
cd ../..
export `heroku config -a gainy-fetch-dev -s`
export HASURA_PORT=5000
docker-compose up meltano -d 
docker-compose exec meltano meltano schedule run eodhistoricaldata-to-postgres-0 # to run stage 0 of load and transform
docker-compose exec meltano meltano invoke dbt run # to run transformation
```
