import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from common import make_graphql_request, db_connect

PROFILES = make_graphql_request("{app_profiles{id, user_id}}",
                                user_id=None)['data']['app_profiles']

query_names = [
    'CreatePlaidLinkToken',
    'LinkManagedTradingBankAccountWithPlaid',
    'ManagedPortfolioGetFundingAccountsWithUpdatedBalance',
    'ManagedPortfolioDeleteFundingAccount',
]
QUERIES = {}
for query_name in query_names:
    query_file = os.path.join(os.path.dirname(__file__),
                              'queries/connecting_bank_account',
                              query_name + '.graphql')
    with open(query_file, 'r') as f:
        QUERIES[query_name] = f.read()


def test_create_plaid_link_token():
    profile_id = PROFILES[0]['id']
    profile_user_id = PROFILES[0]['user_id']

    data = make_graphql_request(
        QUERIES['CreatePlaidLinkToken'], {
            "profile_id": profile_id,
            "redirect_uri": "https://app.gainy.application.ios",
            "env": "sandbox",
            "purpose": "managed_trading",
        }, profile_user_id)['data']['create_plaid_link_token']

    assert "link_token" in data
    assert data["link_token"] is not None


def test_full_flow():
    profile_id = PROFILES[0]['id']
    profile_user_id = PROFILES[0]['user_id']

    access_token = 'access-sandbox-23e0b768-697b-4c89-ad1a-9ccf0d49f0be'
    item_id = 'XvQARLzQKBh79bJ74n39twjGEGQVZpudxZgw3'
    account_id = "elyBD9bye1SP8GMPvap8cXLKLQB6nXfVlWoMQ"

    query = """INSERT INTO app.profile_plaid_access_tokens (profile_id, access_token, item_id, purpose)
    VALUES (%(profile_id)s, %(access_token)s, %(item_id)s, %(purpose)s)
    RETURNING id
    """
    with db_connect() as db_conn:
        with db_conn.cursor() as cursor:
            cursor.execute(
                query, {
                    "profile_id": profile_id,
                    "access_token":
                    'access-sandbox-23e0b768-697b-4c89-ad1a-9ccf0d49f0be',
                    "item_id": 'XvQARLzQKBh79bJ74n39twjGEGQVZpudxZgw3',
                    "purpose": 'managed_trading',
                })
            access_token_id = cursor.fetchone()[0]

    data = make_graphql_request(
        QUERIES['LinkManagedTradingBankAccountWithPlaid'], {
            "profile_id": profile_id,
            "account_id": account_id,
            "account_name": "test",
            "access_token_id": access_token_id,
        }, profile_user_id
    )['data']['link_managed_trading_bank_account_with_plaid']

    assert "error_message" in data
    assert data["error_message"] is None
    assert "funding_account" in data
    assert data["funding_account"] is not None

    funding_account_id = data["funding_account"]["id"]

    data = make_graphql_request(
        QUERIES['ManagedPortfolioGetFundingAccountsWithUpdatedBalance'], {
            "profile_id": profile_id,
        }, profile_user_id)['data']['managed_portfolio_get_funding_accounts']

    funding_account_ids = [i["funding_account"]["id"] for i in data]
    assert funding_account_id in funding_account_ids

    data = make_graphql_request(
        QUERIES['ManagedPortfolioDeleteFundingAccount'], {
            "profile_id": profile_id,
            "funding_account_id": funding_account_id,
        }, profile_user_id)['data']['managed_portfolio_delete_funding_account']

    assert "ok" in data
    assert data["ok"]
