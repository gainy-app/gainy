#!/bin/sh

( cd scripts && python3 generate_collection_rules_sql.py )

meltano ui
