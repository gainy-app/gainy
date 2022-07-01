import sys, os, json, yaml, copy, re
import glob
from typing import List
from operator import itemgetter
from gainy.utils import db_connect
import psycopg2


def _split_schedule(tap: str, tap_canonical: str, template, env, split_id,
                    split_num) -> dict:
    new_schedule = copy.deepcopy(template)
    new_schedule["name"] = '%s-to-postgres-%02d' % (tap, split_id)

    if "env" not in new_schedule:
        new_schedule["env"] = {}

    if split_num and split_num > 1:
        new_schedule["env"][f"TAP_{tap_canonical}_SPLIT_ID"] = str(split_id)
        new_schedule["env"][f"TAP_{tap_canonical}_SPLIT_NUM"] = str(split_num)

    return new_schedule


def _generate_schedules(env):
    schedules = config['schedules']
    for tap in [
            'eodhistoricaldata', 'eodhistoricaldata-prices', 'coingecko',
            'polygon'
    ]:
        schedules_to_split = list(
            filter(lambda x: x['name'].startswith(f'{tap}-to-postgres'),
                   schedules))
        other_schedules = list(
            filter(lambda x: not x['name'].startswith(f'{tap}-to-postgres'),
                   schedules))

        if not schedules_to_split:
            continue

        tap_canonical = re.sub(r'\W', '_', tap).upper()
        split_num_env_var_name = f'{tap_canonical}_JOBS_COUNT'
        if split_num_env_var_name not in os.environ:
            continue
        split_num = int(os.environ[split_num_env_var_name])

        schedule_split_template = schedules_to_split[0]
        new_split_schedules = [
            _split_schedule(tap, tap_canonical, schedule_split_template, env,
                            k, split_num) for k in range(0, split_num)
        ]

        schedules = other_schedules + new_split_schedules

    try:
        with db_connect() as db_conn:
            with db_conn.cursor() as cursor:
                cursor.execute(
                    "SELECT contract_name FROM ticker_options_monitored")
                option_contract_names = map(itemgetter(0), cursor.fetchall())
    except psycopg2.errors.UndefinedTable:
        option_contract_names = []

    try:
        with db_connect() as db_conn:
            with db_conn.cursor() as cursor:
                cursor.execute("""
                    select code
                    from raw_data.eod_historical_prices
                    join base_tickers on base_tickers.symbol = eod_historical_prices.code
                    where adjusted_close < 0 and base_tickers.type != 'crypto'
                    group by code
                    """)
                full_refresh_symbols = list(
                    map(itemgetter(0), cursor.fetchall()))
    except psycopg2.errors.UndefinedTable:
        full_refresh_symbols = []

    for schedule in schedules:
        if schedule['name'].startswith('polygon-to-postgres'):
            if "env" not in schedule:
                schedule["env"] = {}
            schedule['env']['TAP_POLYGON_OPTION_CONTRACT_NAMES'] = ",".join(
                option_contract_names)

        if schedule['extractor'].startswith('tap-eodhistoricaldata'):
            if "env" not in schedule:
                schedule["env"] = {}
            schedule['env'][
                'TAP_EODHISTORICALDATA_FULL_REFRESH_SYMBOLS'] = ",".join(
                    full_refresh_symbols)

    return schedules


def _generate_analytics_tap_catalog():
    with open("scripts/templates/analytics_tap_catalog_schemas.template.json",
              "r") as f:
        schemas = json.load(f)

    streams = {
        'default': [],
        'small-batch': [],
    }

    for schema_name, tables in schemas.items():
        for table_name, table_config in tables.items():
            if table_config.get('isSmallBatch'):
                stream_type = 'small-batch'
            else:
                stream_type = 'default'

            streams[stream_type].append({
                "table_name":
                table_name,
                "stream":
                table_name,
                "metadata": [{
                    "breadcrumb": [],
                    "metadata": {
                        "replication-method": "FULL_TABLE",
                        "schema-name": schema_name,
                        "database-name": "gainy",
                        "selected": True,
                        **table_config['metadata']
                    }
                }],
                "tap_stream_id":
                f"{schema_name}-{table_name}",
                "schema": {
                    "type": "object"
                }
            })

    os.makedirs("catalog/analytics", exist_ok=True)
    with open("catalog/analytics/tap.catalog.json", "w") as f:
        json.dump({"streams": streams['default']}, f)
    with open("catalog/analytics/tap-small-batch.catalog.json", "w") as f:
        json.dump({"streams": streams['small-batch']}, f)


#####   Configure and run   #####

if 'ENV' not in os.environ:
    raise Exception('env var ENV must be set')
ENV = os.environ['ENV']

if 'DBT_TARGET_SCHEMA' not in os.environ:
    raise Exception('env var DBT_TARGET_SCHEMA must be set')
DBT_TARGET_SCHEMA = os.environ['DBT_TARGET_SCHEMA']

### Meltano config ###

with open("meltano.template.yml", "r") as f:
    config = yaml.safe_load(f)

config['schedules'] = _generate_schedules(ENV)

with open("meltano.yml", "w") as f:
    yaml.dump(config, f)

_generate_analytics_tap_catalog()

if DBT_TARGET_SCHEMA != 'public':
    ### Algolia search mapping ###

    with open("catalog/search/search.mapping.yml", "r") as f:
        config = f.read()

    config = re.sub(r'schema: public\w*', f'schema: {DBT_TARGET_SCHEMA}',
                    config)

    with open("catalog/search/search.mapping.yml", "w") as f:
        f.write(config)

    ### Taps' catalogs ###

    for filename in glob.glob("catalog/**/*.catalog.json", recursive=True):
        with open(filename, "r") as f:
            config = json.load(f)

        for stream in config['streams']:
            for metadata in stream['metadata']:
                if 'metadata' not in metadata:
                    continue
                if 'schema-name' not in metadata['metadata']:
                    continue
                if metadata['metadata']['schema-name'] != 'public':
                    continue
                metadata['metadata']['schema-name'] = DBT_TARGET_SCHEMA
                stream[
                    'tap_stream_id'] = f"{DBT_TARGET_SCHEMA}-{stream['stream']}"

        with open(filename, "w") as f:
            json.dump(config, f)
