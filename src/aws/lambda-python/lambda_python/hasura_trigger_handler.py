from common.hasura_dispatcher import HasuraTriggerDispatcher

from trigger.set_user_categories import SetUserCategories

from . import API_GATEWAY_PROXY_INTEGRATION, DB_CONN_STRING

TRIGGERS = [
    SetUserCategories()
]

dispatcher = HasuraTriggerDispatcher(
    DB_CONN_STRING,
    TRIGGERS,
    API_GATEWAY_PROXY_INTEGRATION
)


def handle(event, context):
    dispatcher.handle(event, context)
