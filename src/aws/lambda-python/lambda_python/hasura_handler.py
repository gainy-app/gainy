import os

from common.hasura_dispatcher import HasuraActionDispatcher, HasuraTriggerDispatcher
from recommendation.recommendation_action import GetMatchScoreByTicker, GetRecommendedCollections, \
    GetMatchScoreByCollections

# DB CONNECTION
from trigger.set_user_categories import SetUserCategories

HOST = os.environ['pg_host']
PORT = os.environ['pg_port']
DB_NAME = os.environ['pg_dbname']
USERNAME = os.environ['pg_username']
PASSWORD = os.environ['pg_password']

DB_CONN_STRING = f"postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}"

# API GATEWAY PROXY INTEGRATION
API_GATEWAY_PROXY_INTEGRATION = os.environ.get("AWS_LAMBDA_API_GATEWAY_PROXY_INTEGRATION", "True") == "True"


ACTIONS = [
    GetRecommendedCollections(),
    GetMatchScoreByTicker(),
    GetMatchScoreByCollections()
]

action_dispatcher = HasuraActionDispatcher(
    DB_CONN_STRING,
    ACTIONS,
    API_GATEWAY_PROXY_INTEGRATION
)


def handle_action(event, context):
    action_dispatcher.handle(event, context)


TRIGGERS = [
    SetUserCategories()
]

trigger_dispatcher = HasuraTriggerDispatcher(
    DB_CONN_STRING,
    TRIGGERS,
    API_GATEWAY_PROXY_INTEGRATION
)


def handle_trigger(event, context):
    trigger_dispatcher.handle(event, context)

