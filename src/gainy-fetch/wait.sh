#!/bin/sh

bash -c 'while !</dev/tcp/meltano-airflow-ui/5001; do sleep 1; done;'

meltano "$@"