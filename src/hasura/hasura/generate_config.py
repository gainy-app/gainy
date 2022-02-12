import os, re, yaml

if 'HASURA_GRAPHQL_PUBLIC_SCHEMA_NAME' not in os.environ:
    raise Exception('env var HASURA_GRAPHQL_PUBLIC_SCHEMA_NAME must be set')
HASURA_GRAPHQL_PUBLIC_SCHEMA_NAME = os.environ['HASURA_GRAPHQL_PUBLIC_SCHEMA_NAME']

def should_replace_schema(schema):
    return re.match(r'^public', schema) is not None

def generate_tables():
    with open("metadata/tables.yaml", "r") as f:
        tables = yaml.safe_load(f)

    for table in tables:
        if should_replace_schema(table['table']['schema']):
            table['table']['schema'] = HASURA_GRAPHQL_PUBLIC_SCHEMA_NAME
            if 'configuration' not in table:
                table['configuration'] = {}
            if 'custom_name' not in table['configuration']:
                table['configuration']['custom_name'] = table['table']['name']

        for key in ['array_relationships', 'object_relationships']:
            if key not in table:
                continue

            for i in table[key]:
                if 'manual_configuration' not in i['using']:
                    continue
                if not should_replace_schema(i['using']['manual_configuration']['remote_table']['schema']):
                    continue
                i['using']['manual_configuration']['remote_table']['schema'] = HASURA_GRAPHQL_PUBLIC_SCHEMA_NAME

    with open("metadata/tables.yaml", "w") as f:
        yaml.dump(tables, f)

def generate_actions():
    with open("metadata/actions.yaml", "r") as f:
        actions = yaml.safe_load(f)

    for object_config in actions['custom_types']['objects']:
        if 'relationships' not in object_config:
            continue

        for relationship in object_config['relationships']:
            if 'remote_table' not in relationship or 'schema' not in relationship['remote_table'] or not should_replace_schema(relationship['remote_table']['schema']):
                continue

            relationship['remote_table']['schema'] = HASURA_GRAPHQL_PUBLIC_SCHEMA_NAME

    with open("metadata/actions.yaml", "w") as f:
        yaml.dump(actions, f)

generate_tables()
generate_actions()