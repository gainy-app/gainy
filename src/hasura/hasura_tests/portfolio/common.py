from psycopg2.extras import RealDictCursor

from hasura_tests.common import make_graphql_request, db_connect

PROFILES = make_graphql_request("{app_profiles{id, user_id}}",
                                user_id=None)['data']['app_profiles']
PROFILE_IDS = {profile['user_id']: profile['id'] for profile in PROFILES}


def get_test_portfolio_data(only_with_holdings=False):
    query = "select profile_id, min(date) as min_date, max(date) as max_date from app.profile_portfolio_transactions group by profile_id"
    transaction_stats = {}
    with db_connect() as db_conn:
        with db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            for row in cursor:
                transaction_stats[row["profile_id"]] = {
                    'min_date': row['min_date'].strftime('%Y-%m-%d'),
                    'max_date': row['max_date'].strftime('%Y-%m-%d'),
                }

    quantities = {"AAPL": 100, "AAPL240621C00225000": 200}
    quantities2 = {"AAPL": 110, "AAPL240621C00225000": 300}
    quantities3 = {"AAPL": 10, "AAPL240621C00225000": 100}

    # -- profile 1 with holdings without transactions at all
    quantities_override = [
        (None, "2022-03-10T00:00:00", {
            "AAPL": 100,
            "AAPL240621C00225000": 0
        }),
    ]
    yield ('user_id_portfolio_test_1', quantities, quantities_override)

    # -- profile 2 with holdings with one buy transaction on the primary account
    user_id = 'user_id_portfolio_test_2'
    quantities_override = [
        (None,
         transaction_stats[PROFILE_IDS[user_id]]['min_date'], 0),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 3 with holdings with one sell transaction on the primary account
    user_id = 'user_id_portfolio_test_3'
    quantities_override = [
        (None,
         transaction_stats[PROFILE_IDS[user_id]]['min_date'], 0),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 4 with holdings with one buy transaction on the secondary account
    user_id = 'user_id_portfolio_test_4'
    quantities_override = [
        (None,
         transaction_stats[PROFILE_IDS[user_id]]['min_date'], 0),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 5 with holdings with one sell transaction on the secondary account
    user_id = 'user_id_portfolio_test_5'
    quantities_override = [
        (None,
         transaction_stats[PROFILE_IDS[user_id]]['min_date'], 0),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 6 with holdings with buy-sell transactions on the primary account
    user_id = 'user_id_portfolio_test_6'
    quantities_override = [
        (None,
         transaction_stats[PROFILE_IDS[user_id]]['min_date'], 0),
        (transaction_stats[PROFILE_IDS[user_id]]['min_date'],
         transaction_stats[PROFILE_IDS[user_id]]['max_date'], quantities2),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 7 with holdings with buy-sell transactions on the primary-secondary account
    user_id = 'user_id_portfolio_test_7'
    quantities_override = [
        (None,
         transaction_stats[PROFILE_IDS[user_id]]['min_date'], 0),
        (transaction_stats[PROFILE_IDS[user_id]]['min_date'],
         transaction_stats[PROFILE_IDS[user_id]]['max_date'], quantities2),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 8 with holdings with buy-sell transactions on the secondary-primary account
    user_id = 'user_id_portfolio_test_8'
    quantities_override = [
        (None,
         transaction_stats[PROFILE_IDS[user_id]]['min_date'], 0),
        (transaction_stats[PROFILE_IDS[user_id]]['min_date'],
         transaction_stats[PROFILE_IDS[user_id]]['max_date'], quantities2),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 9 with holdings with buy-sell transactions on the secondary account
    user_id = 'user_id_portfolio_test_9'
    quantities_override = [
        (None,
         transaction_stats[PROFILE_IDS[user_id]]['min_date'], 0),
        (transaction_stats[PROFILE_IDS[user_id]]['min_date'],
         transaction_stats[PROFILE_IDS[user_id]]['max_date'], quantities2),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 10 with holdings with sell-buy transactions on the primary account
    user_id = 'user_id_portfolio_test_10'
    quantities_override = [
        (None,
         transaction_stats[PROFILE_IDS[user_id]]['max_date'], 0),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 11 with holdings with sell-buy transactions on the primary-secondary account
    user_id = 'user_id_portfolio_test_11'
    quantities_override = [
        (None,
         transaction_stats[PROFILE_IDS[user_id]]['min_date'], 0),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 12 with holdings with sell-buy transactions on the secondary-primary account
    user_id = 'user_id_portfolio_test_12'
    quantities_override = [
        (None,
         transaction_stats[PROFILE_IDS[user_id]]['max_date'], 0),
    ]
    yield (user_id, quantities, quantities_override)

    # -- profile 13 with holdings with sell-buy transactions on the secondary account
    user_id = 'user_id_portfolio_test_13'
    quantities_override = [
        (None,
         transaction_stats[PROFILE_IDS[user_id]]['min_date'], 0),
    ]
    yield (user_id, quantities, quantities_override)

    if only_with_holdings:
        return

    quantities = {"AAPL": 0, "AAPL240621C00225000": 0}

    # -- profile 14 without holdings without transactions at all
    # -- profile 15 without holdings with one buy transaction on the primary account
    # -- profile 16 without holdings with one sell transaction on the primary account
    # -- profile 17 without holdings with one buy transaction on the secondary account
    # -- profile 18 without holdings with one sell transaction on the secondary account
    # -- profile 19 without holdings with buy-sell transactions on the primary account
    # -- profile 20 without holdings with buy-sell transactions on the primary-secondary account
    # -- profile 21 without holdings with buy-sell transactions on the secondary-primary account
    # -- profile 22 without holdings with buy-sell transactions on the secondary account
    # -- profile 23 without holdings with sell-buy transactions on the primary account
    # -- profile 24 without holdings with sell-buy transactions on the primary-secondary account
    # -- profile 25 without holdings with sell-buy transactions on the secondary-primary account
    # -- profile 26 without holdings with sell-buy transactions on the secondary account
    for i in range(14, 27):
        if i < 19 or i > 22:
            yield ('user_id_portfolio_test_' + str(i), quantities, [])
        else:
            quantities_override = [
                (transaction_stats[PROFILE_IDS[user_id]]['min_date'], transaction_stats[PROFILE_IDS[user_id]]['max_date'], quantities3),
            ]
            yield ('user_id_portfolio_test_' + str(i), quantities,
                   quantities_override)
