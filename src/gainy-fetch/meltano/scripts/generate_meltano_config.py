import sys, os, json, yaml, copy

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

if 'EODHISTORICALDATA_JOBS_COUNT' not in os.environ:
    raise Exception('env var EODHISTORICALDATA_JOBS_COUNT must be set')
if len(sys.argv) < 2:
    raise Exception('usage: generate_meltano_config.py local|production')

chunks_count = json.loads(os.environ['EODHISTORICALDATA_JOBS_COUNT'])
env = sys.argv[1]
symbols_file = "symbols.%s.json" % (env)

if not os.path.exists(symbols_file):
    raise Exception('%s does not exist' % (symbols_file))

with open(symbols_file, "r") as f:
    symbols = json.load(f)

with open("meltano.template.yml", "r") as f:
    config = yaml.safe_load(f)

eod_schedules = list(filter(lambda x: x['name'].startswith('eodhistoricaldata-to-postgres'), config['schedules']))
non_eod_schedules = list(filter(lambda x: not x['name'].startswith('eodhistoricaldata-to-postgres'), config['schedules']))

if len(eod_schedules) == 0:
    raise Error('no eod schedules found')

eod_schedule_template = eod_schedules[0]
chunk_size = (len(symbols) + chunks_count - 1) // chunks_count # ceil
new_eod_schedules = []
for k,chunk in enumerate(chunks(symbols, chunk_size)):
    new_schedule = copy.deepcopy(eod_schedule_template)
    new_schedule['name'] = 'eodhistoricaldata-to-postgres-%02d' % (k)
    if 'env' not in new_schedule:
        new_schedule['env'] = {}
    new_schedule['env']['TAP_EODHISTORICALDATA_SYMBOLS'] = json.dumps(chunk)
    new_eod_schedules.append(new_schedule)

config['schedules'] = non_eod_schedules + new_eod_schedules

with open("meltano.yml", "w") as f:
    yaml.dump(config, f)