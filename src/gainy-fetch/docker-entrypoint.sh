#!/bin/sh

( cd scripts && python3 generate_rules_sql.py )

meltano ui
