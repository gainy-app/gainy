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
