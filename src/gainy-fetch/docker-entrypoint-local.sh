#!/bin/sh

sh /docker-entrypoint.sh "$1"

meltano invoke airflow users create \
  --username user \
  --password userpass \
  --role Admin \
  --email ironman@avengers.com \
  --firstname Tony \
  --lastname Stark

meltano ui start --bind-port $MELTANO_UI_PORT