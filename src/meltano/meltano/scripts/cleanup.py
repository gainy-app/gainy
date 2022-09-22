import boto3
import datetime
import json
import logging
import os
import psycopg2.errors
import re
import time
from operator import itemgetter

from algoliasearch.search_client import SearchClient
from gainy.utils import db_connect

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

schema_activity_min_datetime = datetime.datetime.now(
    tz=datetime.timezone.utc) - datetime.timedelta(days=3)

AWS_LAMBDA_API_GATEWAY_ENDPOINT = os.getenv("AWS_LAMBDA_API_GATEWAY_ENDPOINT")


def clean_algolia():
    algolia_app_id = os.getenv("ALGOLIA_APP_ID")
    algolia_api_key = os.getenv("ALGOLIA_INDEXING_API_KEY")
    if not algolia_app_id or not algolia_api_key:
        return

    search_client = SearchClient.create(algolia_app_id, algolia_api_key)
    index_names = [
        os.getenv("ALGOLIA_TICKERS_INDEX"),
        os.getenv("ALGOLIA_COLLECTIONS_INDEX")
    ]
    for index_name in index_names:
        if not index_name:
            continue

        search_index = search_client.init_index(index_name)
        search_index.delete_by({
            'numericFilters':
            [f'_sdc_extracted_at<={int(time.time()) - 86400}'],
        })


def clean_api_gateway(api_id, version):

    def get_route_version(route):
        m = re.search(r" /(\d+)/", route)
        if m is None:
            return None

        return m[1]

    client = boto3.client("apigatewayv2")

    integration_ids_to_preserve = []
    paginator = client.get_paginator('get_routes')
    for response in paginator.paginate(ApiId=api_id):
        routes = response['Items']
        for route in routes:
            route_version = get_route_version(route['RouteKey'])

            if route_version is None or int(
                    route_version) >= int(version) - 10:
                integration_id = re.search(r"/(\w+)$", route['Target'])[1]
                integration_ids_to_preserve.append(integration_id)
                continue

            route_id = route['RouteId']
            client.delete_route(ApiId=api_id, RouteId=route_id)
            logger.info(f"Removed route {route_id} {route['RouteKey']}")

    paginator = client.get_paginator('get_integrations')
    for response in paginator.paginate(ApiId=api_id):
        for integration in response['Items']:
            integration_id = integration['IntegrationId']
            if integration_id in integration_ids_to_preserve:
                continue

            try:
                client.delete_integration(ApiId=api_id,
                                          IntegrationId=integration_id)
                logger.info(f"Removed integration {integration_id}")
            except client.exceptions.ConflictException as e:
                pass


def clean_lambda(api_id, env):

    def get_existing_routes():
        client = boto3.client("apigatewayv2")
        paginator = client.get_paginator('get_routes')
        res = []
        for response in paginator.paginate(ApiId=api_id):
            routes = response['Items']
            res += [re.sub(r'^\w* ', '', i['RouteKey']) for i in routes]

        return set(res)

    existing_routes = get_existing_routes()
    client = boto3.client("lambda")

    functions = client.list_functions()['Functions']
    for function in functions:
        if not re.search(f'_{env}$', function['FunctionName']):
            continue

        for version in client.list_versions_by_function(
                FunctionName=function['FunctionName'])['Versions']:
            function_name = function['FunctionName'] + (
                f":{version['Version']}"
                if version['Version'] != '$LATEST' else '')
            try:
                policy = client.get_policy(FunctionName=function_name)
            except client.exceptions.ResourceNotFoundException as e:
                continue

            policy_data = json.loads(policy['Policy'])

            for statement in policy_data['Statement']:
                m = re.search(
                    r'(/\w*){2}$',
                    statement['Condition']['ArnLike']['AWS:SourceArn'])
                if m is None:
                    continue

                route = m[0]
                if route in existing_routes:
                    continue

                client.remove_permission(FunctionName=function_name,
                                         StatementId=statement['Sid'])
                logger.info(
                    f"Removed Lambda Policy Statement {statement['Sid']} for route {route}"
                )


def clean_schemas(db_conn):
    with db_conn.cursor() as cursor:
        cursor.execute(
            "SELECT schema_name FROM deployment.public_schemas WHERE deleted_at is null"
        )
        schemas = map(itemgetter(0), cursor.fetchall())

    # we only select schemas starting with "public" and with some suffix
    schemas = filter(lambda x: re.match(r'^public.+', x), schemas)

    schemas = list(schemas)

    if len(schemas) == 0:
        return

    logger.info('Considering schemas %s', schemas)

    schema_last_activity_at = {}
    for schema in schemas:
        with db_conn.cursor() as cursor:
            try:
                cursor.execute(
                    f"SELECT last_activity_at FROM {schema}.deployment_metadata"
                )
                last_activity_at = cursor.fetchone()[0]
                schema_last_activity_at[schema] = last_activity_at
            except psycopg2.errors.UndefinedTable:
                db_conn.rollback()

    if not len(schema_last_activity_at):
        raise Exception('No schemas found')

    max_last_activity_at = max(schema_last_activity_at.values())

    for schema in schemas:
        if schema in schema_last_activity_at:
            last_activity_at = schema_last_activity_at[schema]
            if last_activity_at == max_last_activity_at:
                logger.info('Skipping schema %s: most recent schema', schema)
                continue
            if last_activity_at > schema_activity_min_datetime:
                logger.info('Skipping schema %s: too recent', schema)
                continue

        query = f"drop schema {schema} cascade"
        logger.warning(query)
        with db_conn.cursor() as cursor:
            try:
                cursor.execute(query)
            except (psycopg2.errors.UndefinedTable,
                    psycopg2.errors.InvalidSchemaName):
                db_conn.rollback()

            cursor.execute(
                "update deployment.public_schemas set deleted_at = now() where schema_name = %(schema)s",
                {"schema": schema})


def clean_obsolete_data(db_conn):
    queries = [
        "delete from meltano.runs where ended_at < now() - interval '1 month'",
        "delete from raw_data.eod_intraday_prices where time < now() - interval '2 weeks'",
        "delete from raw_data.polygon_intraday_prices where time < now() - interval '2 weeks'",
        "delete from deployment.realtime_listener_heartbeat where time < now() - interval '1 hour'",
    ]

    for query in queries:
        with db_conn.cursor() as cursor:
            logger.warning(query)
            cursor.execute(query)


clean_algolia()

with db_connect() as db_conn:
    clean_schemas(db_conn)
    clean_obsolete_data(db_conn)

if AWS_LAMBDA_API_GATEWAY_ENDPOINT is not None:
    res = re.search(r"https://([^.]+)\..*_(\w+)/([^/]+)$",
                    AWS_LAMBDA_API_GATEWAY_ENDPOINT)
    if res is not None:
        api_id = res[1]
        env = res[2]
        version = res[3]
        logger.info(str([api_id, env, version]))

        clean_api_gateway(api_id, version)
        clean_lambda(api_id, env)
