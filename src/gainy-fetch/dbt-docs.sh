#!/bin/sh

meltano invoke dbt docs generate
sh /wait.sh "$@"