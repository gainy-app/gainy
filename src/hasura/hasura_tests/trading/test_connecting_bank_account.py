import pytest

from hasura_tests.common import make_graphql_request, db_connect, load_query
from hasura_tests.trading.common import fill_kyc_form, kyc_send_form, PROFILES

profile_id = PROFILES[0]['id']
profile_user_id = PROFILES[0]['user_id']


def test_create_plaid_link_token():
    data = make_graphql_request(
        load_query('trading/queries/connecting_bank_account',
                   'CreatePlaidLinkToken'), {
                       "profile_id": profile_id,
                       "redirect_uri": "https://app.gainy.application.ios",
                       "env": "sandbox",
                       "purpose": "trading",
                   }, profile_user_id)['data']['create_plaid_link_token']

    assert "link_token" in data
    assert data["link_token"] is not None


@pytest.mark.drivewealth
def test_full_flow():
    fill_kyc_form(profile_id, profile_user_id)
    kyc_send_form(profile_id, profile_user_id)

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
                    "access_token": access_token,
                    "item_id": item_id,
                    "purpose": 'trading',
                })
            access_token_id = cursor.fetchone()[0]

    data = make_graphql_request(
        load_query('trading/queries/connecting_bank_account',
                   'LinkBankAccountWithPlaid'), {
                       "profile_id": profile_id,
                       "account_id": account_id,
                       "account_name": "test",
                       "access_token_id": access_token_id,
                   },
        profile_user_id)['data']['trading_link_bank_account_with_plaid']

    assert "funding_account" in data
    assert data["funding_account"] is not None

    funding_account_id = data["funding_account"]["id"]

    data = make_graphql_request(
        load_query('trading/queries/connecting_bank_account',
                   'GetFundingAccountsWithUpdatedBalance'), {
                       "profile_id": profile_id,
                   }, profile_user_id)['data']['trading_get_funding_accounts']

    funding_account_ids = [i["funding_account"]["id"] for i in data]
    assert funding_account_id in funding_account_ids

    data = make_graphql_request(
        load_query('trading/queries/connecting_bank_account',
                   'DeleteFundingAccount'), {
                       "profile_id": profile_id,
                       "funding_account_id": funding_account_id,
                   },
        profile_user_id)['data']['trading_delete_funding_account']

    assert "ok" in data
    assert data["ok"]
