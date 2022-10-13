from hasura_handler import action_dispatcher, trigger_dispatcher
from tests.common import get_action_event, get_trigger_event, PROFILE_ID, PROFILE_ID2, USER_ID, USER_ID2, PLAID_ACCESS_TOKEN, PLAID_ITEM_ID

MATCH_SCORE_FIELDS_SET = {
    'symbol',
    'match_score',
    'fits_risk',
    'risk_similarity',
    'fits_categories',
    'fits_interests',
    'category_matches',
    'interest_matches',
    'matches_portfolio',
}


# Actions
def test_set_recommendation_settings():
    event = get_action_event(
        "set_recommendation_settings", {
            "profile_id": PROFILE_ID,
            'interests': [1],
            'categories': [1],
            'recommended_collections_count': 1
        }, USER_ID)
    response = action_dispatcher.handle(event)
    assert "code" not in response
    assert "recommended_collections" in response
    assert isinstance(response["recommended_collections"], list)


def test_get_recommended_collections():
    event = get_action_event("get_recommended_collections",
                             {"profile_id": PROFILE_ID}, USER_ID)
    response = action_dispatcher.handle(event)
    assert "code" not in response
    assert isinstance(response, list)


def test_create_plaid_link_token():
    event = get_action_event(
        "create_plaid_link_token", {
            "profile_id": PROFILE_ID,
            "redirect_uri": "https://app.gainy.application.ios",
            "env": "sandbox",
            "purpose": "portfolio",
        }, USER_ID)
    response = action_dispatcher.handle(event)
    assert "code" not in response
    assert isinstance(response, dict)
    assert 'link_token' in response


def test_get_portfolio_chart():
    event = get_action_event("get_portfolio_chart", {"profile_id": PROFILE_ID},
                             USER_ID)
    response = action_dispatcher.handle(event)
    assert "code" not in response
    assert isinstance(response, list)
    assert len(response)
    assert set({
        'period', 'datetime', 'open', 'high', 'low', 'close', 'adjusted_close'
    }).issubset(set(response[0].keys()))


def test_get_portfolio_chart_previous_period_close():
    event = get_action_event("get_portfolio_chart_previous_period_close",
                             {"profile_id": PROFILE_ID}, USER_ID)
    response = action_dispatcher.handle(event)
    assert "code" not in response
    assert isinstance(response, dict)
    assert set({
        'prev_close_1d', 'prev_close_1w', 'prev_close_1m', 'prev_close_3m',
        'prev_close_1y', 'prev_close_5y'
    }).issubset(set(response.keys()))


def test_get_match_score_by_ticker():
    event = get_action_event("get_match_score_by_ticker", {
        "profile_id": PROFILE_ID,
        "symbol": "AAPL"
    }, USER_ID)
    response = action_dispatcher.handle(event)
    assert "code" not in response
    assert isinstance(response, dict)
    assert MATCH_SCORE_FIELDS_SET.issubset(set(response.keys()))


def test_get_match_scores_by_ticker_list():
    event = get_action_event("get_match_scores_by_ticker_list", {
        "profile_id": PROFILE_ID,
        "symbols": ["AAPL"]
    }, USER_ID)
    response = action_dispatcher.handle(event)
    assert "code" not in response
    assert isinstance(response, list)
    assert len(response)
    assert MATCH_SCORE_FIELDS_SET.issubset(set(response[0].keys()))


def test_get_match_scores_by_collection():
    event = get_action_event("get_match_scores_by_collection", {
        "profile_id": PROFILE_ID,
        "collection_id": 83
    }, USER_ID)
    response = action_dispatcher.handle(event)
    assert "code" not in response
    assert isinstance(response, list)
    assert len(response)
    assert MATCH_SCORE_FIELDS_SET.issubset(set(response[0].keys()))


def test_get_promocode():
    event = get_action_event("get_promocode", {"code": 'EEKUA1AE'}, USER_ID)
    response = action_dispatcher.handle(event)
    assert "code" not in response
    assert isinstance(response, dict)
    assert set({'id', 'name', 'description',
                'config'}).issubset(set(response.keys()))


# Triggers
def test_set_user_categories():
    event = get_trigger_event(
        "set_user_categories", "insert", {
            "old": {},
            "new": {
                "profile_id": PROFILE_ID,
                "risk_level": 0.5,
                "average_market_return": 6,
                "investment_horizon": 0.5,
                "unexpected_purchases_source": "checking_savings",
                "damage_of_failure": 0.5,
                "stock_market_risk_level": "very_risky",
                "trading_experience": "dont_trade_after_bad_experience",
                "if_market_drops_20_i_will_buy": 0.5,
                "if_market_drops_40_i_will_buy": 0.5,
            }
        }, USER_ID)

    response = trigger_dispatcher.handle(event)
    assert response is None


def test_on_user_created():
    event = get_trigger_event("on_user_created", "insert", {
        "old": {},
        "new": {
            "id": PROFILE_ID,
            "email": "",
        }
    }, USER_ID)

    response = trigger_dispatcher.handle(event)
    assert response is None


def test_set_recommendations():
    for trigger_name in [
            'recommendations__profile_categories',
            'recommendations__profile_interests'
    ]:
        event = get_trigger_event(trigger_name, "insert", {
            "old": {},
            "new": {
                "profile_id": PROFILE_ID,
            }
        }, USER_ID)

        response = trigger_dispatcher.handle(event)
        assert response is None


def test_on_plaid_access_token_created():
    event = get_trigger_event(
        "on_plaid_access_token_created", "insert", {
            "old": {},
            "new": {
                "id": 1,
                "profile_id": PROFILE_ID,
                "access_token": PLAID_ACCESS_TOKEN,
                "item_id": PLAID_ITEM_ID,
            }
        }, USER_ID)

    response = trigger_dispatcher.handle(event)
    assert isinstance(response, dict)
    assert "code" not in response
    assert "holdings_count" in response
    assert "transactions_count" in response


def test_on_invitation_created_or_updated():
    event = get_trigger_event(
        "on_invitation_created_or_updated", "insert", {
            "old": {},
            "new": {
                "id": 1,
                "from_profile_id": PROFILE_ID,
                "to_profile_id": PROFILE_ID,
            }
        }, USER_ID)

    response = trigger_dispatcher.handle(event)
    assert response is None


# Plaid Actions
def test_get_portfolio_holdings():
    event = get_action_event("get_portfolio_holdings",
                             {"profile_id": PROFILE_ID2}, USER_ID2)
    response = action_dispatcher.handle(event)
    assert "code" not in response
    assert isinstance(response, list)
    assert len(response)


def test_get_portfolio_transactions():
    event = get_action_event("get_portfolio_transactions",
                             {"profile_id": PROFILE_ID2}, USER_ID2)
    response = action_dispatcher.handle(event)
    assert "code" not in response
    assert isinstance(response, list)
    assert len(response)
