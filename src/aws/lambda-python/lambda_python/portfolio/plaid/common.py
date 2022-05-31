import os
import json
import plaid
from plaid.api import plaid_api
from common.hasura_exception import HasuraActionException
from services.logging import get_logger

logger = get_logger(__name__)

PLAID_CLIENT_ID = os.getenv('PLAID_CLIENT_ID')
PLAID_SECRET = os.getenv('PLAID_SECRET')
PLAID_DEVELOPMENT_SECRET = os.getenv('PLAID_DEVELOPMENT_SECRET')
PLAID_ENV = os.getenv('PLAID_ENV')
PLAID_HOSTS = {
    'sandbox': plaid.Environment.Sandbox,
    'development': plaid.Environment.Development,
    'production': plaid.Environment.Production,
}

if PLAID_ENV not in PLAID_HOSTS:
    raise Error('Wrong plaid env %s, available options are: %s' %
                (PLAID_ENV, ",".join(keys(PLAID_HOSTS))))


def get_plaid_client(env=None):
    if env is None:
        env = PLAID_ENV

    host = PLAID_HOSTS[env]
    secret = PLAID_DEVELOPMENT_SECRET if env == 'development' and PLAID_DEVELOPMENT_SECRET else PLAID_SECRET

    configuration = plaid.Configuration(host=host,
                                        api_key={
                                            'clientId': PLAID_CLIENT_ID,
                                            'secret': secret,
                                        })

    api_client = plaid.ApiClient(configuration)
    client = plaid_api.PlaidApi(api_client)

    return client


def handle_error(e):
    logger.error('Plaid Error: %s' % (e.body))
    error = json.loads(e.body)

    raise HasuraActionException(
        400, "Plaid error: %s" %
        (error['display_message'] or error['error_message']))
