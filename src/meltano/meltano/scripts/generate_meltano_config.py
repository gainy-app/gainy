import sys, os, json, yaml, copy, re
import glob
from typing import List


def _split_schedule(tap: str, template, env, split_id, split_num) -> dict:
    new_schedule = copy.deepcopy(template)
    new_schedule["name"] = '%s-to-postgres-%02d' % (tap, split_id)

    if "env" not in new_schedule:
        new_schedule["env"] = {}

    if split_num and split_num > 1:
        new_schedule["env"][f"TAP_{tap.upper()}_SPLIT_ID"] = str(split_id)
        new_schedule["env"][f"TAP_{tap.upper()}_SPLIT_NUM"] = str(split_num)

    return new_schedule


def _generate_schedules(env):
    schedules = config['schedules']
    for tap in ['eodhistoricaldata', 'eodhistoricaldata-prices', 'coingecko']:
        schedules_to_split = list(
            filter(lambda x: x['name'].startswith(f'{tap}-to-postgres'),
                   schedules))
        other_schedules = list(
            filter(lambda x: not x['name'].startswith(f'{tap}-to-postgres'),
                   schedules))

        if not schedules_to_split:
            continue

        tap_canonical = re.sub(r'\W', '-_', tap)
        split_num_env_var_name = f'{tap_canonical.upper()}_JOBS_COUNT'
        if split_num_env_var_name not in os.environ:
            continue
        split_num = int(os.environ[split_num_env_var_name])

        schedule_split_template = schedules_to_split[0]
        new_split_schedules = [
            _split_schedule(tap, schedule_split_template, env, k, split_num)
            for k in range(0, split_num)
        ]

        schedules = other_schedules + new_split_schedules
    return schedules


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
