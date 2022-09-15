from hasura_tests.common import make_graphql_request, db_connect
from hasura_tests.trading.common import load_query, PROFILES

profile_id = PROFILES[1]['id']


def test_trading_sync_provider_data():
    query_money = """select count(*) 
    from app.drivewealth_accounts_money 
    join app.drivewealth_accounts on drivewealth_accounts.ref_id = drivewealth_accounts_money.drivewealth_account_id
    join app.drivewealth_users on drivewealth_users.ref_id = drivewealth_accounts.drivewealth_user_id
    where profile_id = %(profile_id)s"""

    query_positions = """select count(*) 
    from app.drivewealth_accounts_positions 
    join app.drivewealth_accounts on drivewealth_accounts.ref_id = drivewealth_accounts_positions.drivewealth_account_id
    join app.drivewealth_users on drivewealth_users.ref_id = drivewealth_accounts.drivewealth_user_id
    where profile_id = %(profile_id)s"""

    with db_connect() as db_conn:
        with db_conn.cursor() as cursor:
            cursor.execute(query_money, {"profile_id": profile_id})
            money_cnt_before = cursor.fetchone()[0]
            cursor.execute(query_positions, {"profile_id": profile_id})
            positions_cnt_before = cursor.fetchone()[0]

            make_graphql_request(
                load_query('trading/queries/debug', 'TradingSyncProviderData'),
                {
                    "profile_id": profile_id,
                }, None)

            cursor.execute(query_money, {"profile_id": profile_id})
            money_cnt_after = cursor.fetchone()[0]
            cursor.execute(query_positions, {"profile_id": profile_id})
            positions_cnt_after = cursor.fetchone()[0]

            assert money_cnt_after == money_cnt_before + 1
            assert positions_cnt_after == positions_cnt_before + 1
