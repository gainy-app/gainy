import json, sys

data = json.load(sys.stdin)

mapping = [
    ('random_password.rds', 'result'),
    ('random_password.airflow', 'result'),
    ('random_password.hasura', 'result'),
    ('random_password.internal_sync_postgres', 'result'),
    ('random_password.rds_external_access', 'result'),
    ('random_integer.db_external_access_port', 'result'),
    ('tls_private_key.bridge', 'private_key_pem'),
]

for key, field in mapping:
    for resource in filter(lambda record: record['type'] + '.' + record['name'] == key, data['resources']):
        print(key)
        print(resource['instances'][0]['attributes'][field])
        print()
