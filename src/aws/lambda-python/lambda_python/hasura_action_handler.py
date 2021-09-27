from common.hasura_dispatcher import HasuraActionDispatcher

from recommendation.recommendation_action import GetMatchScoreByTicker, GetRecommendedCollections, \
    GetMatchScoreByCollections
from . import API_GATEWAY_PROXY_INTEGRATION, DB_CONN_STRING

ACTIONS = [
    GetRecommendedCollections(),
    GetMatchScoreByTicker(),
    GetMatchScoreByCollections()
]

dispatcher = HasuraActionDispatcher(
    DB_CONN_STRING,
    ACTIONS,
    API_GATEWAY_PROXY_INTEGRATION
)


def handle(event, context):
    dispatcher.handle(event, context)
