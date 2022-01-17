import os

from common.hasura_dispatcher import HasuraActionDispatcher, HasuraTriggerDispatcher
from recommendation.match_score_action import GetMatchScoreByCollection, GetMatchScoreByTicker, \
    GetMatchScoreByTickerList
from recommendation.recommendation_action import GetRecommendedCollections
from portfolio.plaid.actions import *
from portfolio.actions import *
from portfolio.triggers import *

# DB CONNECTION
from search.algolia_search import SearchTickers, SearchCollections
from trigger.set_recommendations import SetRecommendations
from trigger.set_user_categories import SetUserCategories
from trigger.on_user_created import OnUserCreated

ENV = os.environ['ENV']

HOST = os.environ['pg_host']
PORT = os.environ['pg_port']
DB_NAME = os.environ['pg_dbname']
USERNAME = os.environ['pg_username']
PASSWORD = os.environ['pg_password']

DB_CONN_STRING = f"postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}"

ALGOLIA_APP_ID = os.environ["ALGOLIA_APP_ID"]
ALGOLIA_TICKERS_INDEX = os.environ["ALGOLIA_TICKERS_INDEX"]
ALGOLIA_COLLECTIONS_INDEX = os.environ["ALGOLIA_COLLECTIONS_INDEX"]
ALGOLIA_SEARCH_API_KEY = os.environ["ALGOLIA_SEARCH_API_KEY"]

API_GATEWAY_PROXY_INTEGRATION = os.environ.get(
    "AWS_LAMBDA_API_GATEWAY_PROXY_INTEGRATION", "True") == "True"

ACTIONS = [
    GetRecommendedCollections(),
    GetMatchScoreByTicker(),
    GetMatchScoreByTickerList(),
    GetMatchScoreByCollection(),

    # Portfolio
    CreatePlaidLinkToken(),
    LinkPlaidAccount(),
    GetPortfolioHoldings(),
    GetPortfolioTransactions(),
    PlaidWebhook(),

    # Search
    SearchTickers(ALGOLIA_APP_ID, ALGOLIA_SEARCH_API_KEY,
                  ALGOLIA_TICKERS_INDEX),
    SearchCollections(ALGOLIA_APP_ID, ALGOLIA_SEARCH_API_KEY,
                      ALGOLIA_COLLECTIONS_INDEX)
]

action_dispatcher = HasuraActionDispatcher(DB_CONN_STRING, ACTIONS,
                                           API_GATEWAY_PROXY_INTEGRATION)


def handle_action(event, context):
    return action_dispatcher.handle(event, context)


TRIGGERS = [
    SetUserCategories(),
    OnUserCreated(ENV),
    SetRecommendations(),
    OnPlaidAccessTokenCreated(),
]

trigger_dispatcher = HasuraTriggerDispatcher(DB_CONN_STRING, TRIGGERS,
                                             API_GATEWAY_PROXY_INTEGRATION)


def handle_trigger(event, context):
    return trigger_dispatcher.handle(event, context)
