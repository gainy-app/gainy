#!/bin/bash

python scripts/generate_meltano_config.py

meltano invoke airflow scheduler "$@"