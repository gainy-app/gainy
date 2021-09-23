#!/bin/sh

bash -c "while ! curl -sD - meltano-airflow-ui:5001 | grep '302 FOUND' > /dev/null; do sleep 1; done;"

meltano "$@"