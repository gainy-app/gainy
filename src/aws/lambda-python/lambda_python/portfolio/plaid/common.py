import os
import json
import plaid
from plaid.api import plaid_api

from common.hasura_exception import HasuraActionException

PLAID_CLIENT_ID = os.environ['PLAID_CLIENT_ID']
PLAID_SECRET = os.environ['PLAID_SECRET']
PLAID_ENV = os.environ['PLAID_ENV']
PLAID_HOSTS = {
    'sandbox': plaid.Environment.Sandbox,
    'development': plaid.Environment.Development,
    'production': plaid.Environment.Production,
}

if PLAID_ENV not in PLAID_HOSTS:
    raise Error('Wrong plaid env %s, available options are: %s' %
                (PLAID_ENV, ",".join(keys(PLAID_HOSTS))))
PLAID_HOST = PLAID_HOSTS[PLAID_ENV]


def get_plaid_client():
    configuration = plaid.Configuration(host=PLAID_HOST,
                                        api_key={
                                            'clientId': PLAID_CLIENT_ID,
                                            'secret': PLAID_SECRET,
                                        })

    api_client = plaid.ApiClient(configuration)
    client = plaid_api.PlaidApi(api_client)

    return client


def handle_error(e):
    print('Plaid Error: %s' % (e.body))
    error = json.loads(e.body)

    raise HasuraActionException(
        400, "Plaid error: %s" %
        (error['display_message'] or error['error_message']))
