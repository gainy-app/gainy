import sys, os, json, yaml, copy, re
import glob
from typing import List


def _fill_in_eod_schedule(template, env, split_id, split_num) -> dict:
    new_schedule = copy.deepcopy(template)
    new_schedule["name"] = 'eodhistoricaldata-to-postgres-%02d' % (split_id)

    if "env" not in new_schedule:
        new_schedule["env"] = {}

    if split_num and split_num > 1:
        new_schedule["env"]["TAP_EODHISTORICALDATA_SPLIT_ID"] = str(split_id)
        new_schedule["env"]["TAP_EODHISTORICALDATA_SPLIT_NUM"] = str(split_num)

    return new_schedule


def _generate_schedules(env, split_num):
    eod_schedules = list(
        filter(lambda x: x['name'].startswith('eodhistoricaldata-to-postgres'),
               config['schedules']))
    non_eod_schedules = list(
        filter(
            lambda x: not x['name'].startswith('eodhistoricaldata-to-postgres'
                                               ), config['schedules']))

    if len(eod_schedules) == 0:
        raise Exception('no eod schedules found')

    eod_schedule_template = eod_schedules[0]
    new_eod_schedules = [
        _fill_in_eod_schedule(eod_schedule_template, env, k, split_num)
        for k in range(0, split_num)
    ]

    return non_eod_schedules + new_eod_schedules


#####   Configure and run   #####

if 'ENV' not in os.environ:
    raise Exception('env var ENV must be set')
ENV = os.environ['ENV']

if 'EODHISTORICALDATA_JOBS_COUNT' not in os.environ:
    raise Exception('env var EODHISTORICALDATA_JOBS_COUNT must be set')
EODHISTORICALDATA_JOBS_COUNT = json.loads(
    os.environ['EODHISTORICALDATA_JOBS_COUNT'])

if 'DBT_TARGET_SCHEMA' not in os.environ:
    raise Exception('env var DBT_TARGET_SCHEMA must be set')
DBT_TARGET_SCHEMA = os.environ['DBT_TARGET_SCHEMA']

### Meltano config ###

with open("meltano.template.yml", "r") as f:
    config = yaml.safe_load(f)

config['schedules'] = _generate_schedules(ENV, EODHISTORICALDATA_JOBS_COUNT)

with open("meltano.yml", "w") as f:
    yaml.dump(config, f)

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
                stream['tap_stream_id'] = f"{DBT_TARGET_SCHEMA}-{stream['stream']}"

        with open(filename, "w") as f:
            json.dump(config, f)
