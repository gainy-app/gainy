import json, sys

data = json.load(sys.stdin)

mapping = [
    ('random_password.rds', 'result'),
    ('random_password.airflow', 'result'),
    ('random_password.hasura', 'result'),
    ('tls_private_key.bridge', 'private_key_pem'),
]

for key, field in mapping:
    resources = filter(lambda record: record['type'] + '.' + record['name'] == key, data['resources'])
    if len(resources):
        print(key)
        print(resources[0]['instances'][0]['attributes'][field])
        print()
