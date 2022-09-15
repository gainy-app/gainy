from decimal import Decimal

import pytest

from tests.repository_mocks import mock_noop
from trading.exceptions import InsufficientFundsException
from trading.drivewealth.api import DriveWealthApi
from trading.drivewealth.models import DriveWealthPortfolio, DriveWealthFund, DriveWealthUser, DriveWealthAccount
from trading.drivewealth.repository import DriveWealthRepository
from trading.drivewealth.provider.collection import DriveWealthProviderCollection
from trading.models import TradingCollectionVersion

_USER_ID = "41dde78c-e31b-43e5-9418-44ae08098738"

_CASH_VALUE = 11001
_CASH_TARGET_WEIGHT = Decimal(0.11)

_FUND1_ID = "fund_b567f1d3-486e-4e5f-aacd-0d551113ebf6"
_FUND1_VALUE = Decimal(33604.12)
_FUND1_TARGET_WEIGHT = Decimal(0.3)
_FUND2_ID = "fund_9a51b36f-faac-41a7-8c9d-b15bb29a05fc"
_FUND2_VALUE = Decimal(65440.5)
_FUND2_TARGET_WEIGHT = Decimal(0.59)
_PORTFOLIO_STATUS = {
    "id":
    "portfolio_24338197-62da-4ac8-a0c9-3204e396f9c7",
    "equity":
    Decimal(110045.62),
    "holdings": [{
        "id": None,
        "type": "CASH_RESERVE",
        "target": _CASH_TARGET_WEIGHT,
        "actual": 0.1,
        "value": _CASH_VALUE,
    }, {
        "id":
        _FUND1_ID,
        "type":
        "FUND",
        "target":
        _FUND1_TARGET_WEIGHT,
        "actual":
        0.3054,
        "value":
        _FUND1_VALUE,
        "holdings": [{
            "instrumentID": "5b85fabb-d57c-44e6-a7f6-a3efc760226c",
            "symbol": "TSLA",
            "target": 0.55,
            "actual": 0.6823,
            "openQty": 62.5213,
            "value": 22928.9
        }, {
            "instrumentID": "a67422af-8504-43df-9e63-7361eb0bd99e",
            "symbol": "AAPL",
            "target": 0.45,
            "actual": 0.3177,
            "openQty": 62.4942,
            "value": 10675.22
        }]
    }, {
        "id": _FUND2_ID,
        "type": "FUND",
        "target": _FUND2_TARGET_WEIGHT,
        "actual": 0.5947,
        "value": _FUND2_VALUE,
        "holdings": []
    }]
}

_PORTFOLIO_REF_ID = "portfolio_24338197-62da-4ac8-a0c9-3204e396f9c7"
_PORTFOLIO = {
    "id":
    _PORTFOLIO_REF_ID,
    "userID":
    _USER_ID,
    "holdings": [{
        "instrumentID": None,
        "type": "CASH_RESERVE",
        "target": _CASH_TARGET_WEIGHT,
    }, {
        "instrumentID": _FUND1_ID,
        "type": "FUND",
        "target": _FUND1_TARGET_WEIGHT,
    }, {
        "instrumentID": _FUND2_ID,
        "type": "FUND",
        "target": _FUND2_TARGET_WEIGHT,
    }]
}

_FUND_WEIGHTS = {
    "symbol_B": Decimal(0.3),
    "symbol_C": Decimal(0.7),
}
_FUND_HOLDINGS = [{
    "instrumentID": "A",
    "target": 0.6
}, {
    "instrumentID": "B",
    "target": 0.4
}]
_NEW_FUND_HOLDINGS = [
    {
        "instrumentID": "A",
        "target": 0,
    },
    {
        "instrumentID": "B",
        "target": 0.3,
    },
    {
        "instrumentID": "C",
        "target": 0.7,
    },
]

_ACCOUNT_ID = "bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557"


def _mock_get_instrument_details(monkeypatch, api):
    api_responses = {
        "symbol_B": {
            "id": "B"
        },
        "symbol_C": {
            "id": "C"
        },
    }

    def mock_get_instrument_details(symbol):
        return api_responses[symbol]

    monkeypatch.setattr(api, "get_instrument_details",
                        mock_get_instrument_details)


def test_get_actual_collection_holdings(monkeypatch):
    profile_id = 1
    collection_id = 2

    user = DriveWealthUser()
    monkeypatch.setattr(user, "ref_id", _USER_ID)

    portfolio = DriveWealthPortfolio(_PORTFOLIO)
    fund = DriveWealthFund()
    monkeypatch.setattr(fund, "ref_id", _FUND1_ID)
    monkeypatch.setattr(fund, "drivewealth_user_id", _USER_ID)

    def mock_get_user(_profile_id):
        assert _profile_id == profile_id
        return user

    def mock_get_user_portfolio(_user_ref_id):
        assert _user_ref_id == _USER_ID
        return portfolio

    def mock_get_user_fund(_user, _collection_id):
        assert _user == user
        assert _collection_id == collection_id
        return fund

    drivewealth_repository = DriveWealthRepository(None)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)
    monkeypatch.setattr(drivewealth_repository, "get_user", mock_get_user)
    monkeypatch.setattr(drivewealth_repository, "get_user_portfolio",
                        mock_get_user_portfolio)
    monkeypatch.setattr(drivewealth_repository, "get_user_fund",
                        mock_get_user_fund)

    api = DriveWealthApi()

    def mock_get_portfolio_status(_portfolio):
        assert _portfolio == portfolio
        return _PORTFOLIO_STATUS

    monkeypatch.setattr(api, "get_portfolio_status", mock_get_portfolio_status)

    service = DriveWealthProviderCollection(drivewealth_repository, api)
    holdings = service.get_actual_collection_holdings(profile_id,
                                                      collection_id)

    expected_holdings = _PORTFOLIO_STATUS["holdings"][1]["holdings"]
    for i in range(2):
        status = holdings[i].get_collection_holding_status()
        assert status.symbol == expected_holdings[i]["symbol"]
        assert status.target_weight == expected_holdings[i]["target"]
        assert status.actual_weight == expected_holdings[i]["actual"]
        assert status.value == expected_holdings[i]["value"]


def test_create_autopilot_run(monkeypatch):
    account = DriveWealthAccount()
    monkeypatch.setattr(account, "ref_id", _ACCOUNT_ID)

    drivewealth_repository = DriveWealthRepository(None)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)

    api = DriveWealthApi()
    data = {
        "id": "ria_rebalance_804d3f5f-a352-4320-992f-c81bf773a739",
        "created": "2019-05-15T19:51:09.147Z",
        "status": "REBALANCE_NOT_STARTED",
        "riaID": "2ffe9863-dd93-4964-87d1-bda90472984f"
    }

    def mock_create_autopilot_run(_account):
        assert _account == account
        return data

    monkeypatch.setattr(api, "create_autopilot_run", mock_create_autopilot_run)

    service = DriveWealthProviderCollection(drivewealth_repository, api)
    autopilot_run = service._create_autopilot_run(account)

    assert autopilot_run.ref_id == data["id"]
    assert autopilot_run.status == data["status"]
    assert autopilot_run.accounts == [_ACCOUNT_ID]
    assert autopilot_run.data == data


def test_upsert_portfolio(monkeypatch):
    profile_id = 1

    user = DriveWealthUser()
    monkeypatch.setattr(user, "ref_id", _USER_ID)
    account = DriveWealthAccount()
    monkeypatch.setattr(account, "ref_id", _ACCOUNT_ID)

    def mock_get_user(_profile_id):
        assert _profile_id == profile_id
        return user

    def mock_get_user_portfolio(_user_ref_id):
        assert _user_ref_id == _USER_ID
        return None

    drivewealth_repository = DriveWealthRepository(None)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)
    monkeypatch.setattr(drivewealth_repository, "get_user", mock_get_user)
    monkeypatch.setattr(drivewealth_repository, "get_user_portfolio",
                        mock_get_user_portfolio)

    api = DriveWealthApi()

    def mock_create_portfolio(_user_id, _name, _client_portfolio_id,
                              _description):
        assert _user_id == _USER_ID
        return _PORTFOLIO

    monkeypatch.setattr(api, "create_portfolio", mock_create_portfolio)

    def mock_update_account(_account_ref_id, _portfolio_ref_id):
        assert _account_ref_id == _ACCOUNT_ID
        assert _portfolio_ref_id == _PORTFOLIO_REF_ID

    monkeypatch.setattr(api, "update_account", mock_update_account)

    service = DriveWealthProviderCollection(drivewealth_repository, api)
    portfolio = service._upsert_portfolio(profile_id, account)

    assert portfolio.ref_id == _PORTFOLIO_REF_ID
    assert portfolio.drivewealth_account_id == _ACCOUNT_ID
    assert portfolio.data == _PORTFOLIO
    assert portfolio.get_fund_weight(_FUND1_ID) == _FUND1_TARGET_WEIGHT


def get_test_upsert_fund():
    return [False, True]


@pytest.mark.parametrize("fund_exists", get_test_upsert_fund())
def test_upsert_fund(fund_exists, monkeypatch):
    profile_id = 1
    collection_id = 2
    trading_collection_version_id = 3
    weights = _FUND_WEIGHTS
    fund_ref_id = "fund_dff726ff-f213-42b1-a759-b20efa3f56d7"

    user = DriveWealthUser()
    monkeypatch.setattr(user, "ref_id", _USER_ID)

    fund = DriveWealthFund()
    monkeypatch.setattr(fund, "ref_id", fund_ref_id)

    def mock_get_user(_profile_id):
        assert _profile_id == profile_id
        return user

    def mock_get_user_fund(_user, _collection_id):
        assert _user == user
        assert _collection_id == collection_id
        return fund if fund_exists else None

    drivewealth_repository = DriveWealthRepository(None)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)
    monkeypatch.setattr(drivewealth_repository, "get_user", mock_get_user)
    monkeypatch.setattr(drivewealth_repository, "get_user_fund",
                        mock_get_user_fund)

    data = {"id": fund_ref_id, "userID": _USER_ID, "holdings": _FUND_HOLDINGS}

    new_fund_holdings = [
        {
            "instrumentID": "B",
            "target": 0.3,
        },
        {
            "instrumentID": "C",
            "target": 0.7,
        },
    ]

    api = DriveWealthApi()
    if fund_exists:

        def mock_update_fund(_fund):
            assert _fund == fund
            assert _fund.holdings == new_fund_holdings
            return data

        monkeypatch.setattr(api, "update_fund", mock_update_fund)
    else:

        def mock_create_fund(_user_id, _name, _client_fund_id, _description,
                             _new_fund_holdings):
            assert _user_id == _USER_ID
            assert _new_fund_holdings == new_fund_holdings
            return data

        monkeypatch.setattr(api, "create_fund", mock_create_fund)
    _mock_get_instrument_details(monkeypatch, api)

    collection_version = TradingCollectionVersion()
    collection_version.profile_id = profile_id
    collection_version.collection_id = collection_id
    collection_version.weights = weights
    collection_version.target_amount_delta = Decimal(0)

    monkeypatch.setattr(collection_version, "id",
                        trading_collection_version_id)
    monkeypatch.setattr(collection_version, "collection_id", collection_id)
    monkeypatch.setattr(collection_version, "weights", weights)

    service = DriveWealthProviderCollection(drivewealth_repository, api)
    fund = service._upsert_fund(profile_id, collection_version)

    if not fund_exists:
        assert fund.ref_id == fund_ref_id
    assert fund.collection_id == collection_id
    assert fund.trading_collection_version_id == trading_collection_version_id
    assert fund.weights == weights
    assert fund.data == data


def test_generate_new_fund_holdings(monkeypatch):
    drivewealth_repository = DriveWealthRepository(None)
    api = DriveWealthApi()

    service = DriveWealthProviderCollection(drivewealth_repository, api)
    fund = DriveWealthFund()
    monkeypatch.setattr(DriveWealthFund, "holdings", _FUND_HOLDINGS)

    _mock_get_instrument_details(monkeypatch, api)

    new_holdings = service._generate_new_fund_holdings(_FUND_WEIGHTS, fund)

    assert new_holdings == [
        {
            "instrumentID": "A",
            "target": 0,
        },
        {
            "instrumentID": "B",
            "target": 0.3,
        },
        {
            "instrumentID": "C",
            "target": 0.7,
        },
    ]


def get_test_handle_cash_amount_change_amounts_ok():
    yield from [100, _CASH_VALUE - 100, _CASH_VALUE]
    yield 0
    yield from [-100, -_CASH_VALUE, -_FUND1_VALUE + 100, -_FUND1_VALUE]


@pytest.mark.parametrize("amount",
                         get_test_handle_cash_amount_change_amounts_ok())
def test_handle_cash_amount_change_ok(amount, monkeypatch):
    amount = Decimal(amount)
    portfolio = DriveWealthPortfolio(_PORTFOLIO)
    drivewealth_repository = DriveWealthRepository(None)
    api = DriveWealthApi()

    def mock_get_portfolio_status(_portfolio):
        assert _portfolio == portfolio
        return _PORTFOLIO_STATUS

    monkeypatch.setattr(api, "get_portfolio_status", mock_get_portfolio_status)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)

    service = DriveWealthProviderCollection(drivewealth_repository, api)
    fund = DriveWealthFund()
    monkeypatch.setattr(fund, "ref_id", _FUND1_ID)

    assert abs(portfolio.cash_target_weight - _CASH_TARGET_WEIGHT) < 1e-3
    assert abs(portfolio.get_fund_weight(_FUND1_ID) -
               _FUND1_TARGET_WEIGHT) < 1e-3

    service._handle_cash_amount_change(amount, portfolio, fund)

    if amount:
        assert abs(portfolio.cash_target_weight -
                   (_CASH_VALUE - amount) / _PORTFOLIO_STATUS['equity']) < 1e-3
        assert abs(
            portfolio.get_fund_weight(_FUND1_ID) -
            (_FUND1_VALUE + amount) / _PORTFOLIO_STATUS['equity']) < 1e-3
    else:
        assert abs(portfolio.cash_target_weight - _CASH_TARGET_WEIGHT) < 1e-3
        assert abs(
            portfolio.get_fund_weight(_FUND1_ID) - _FUND1_TARGET_WEIGHT) < 1e-3


def get_test_handle_cash_amount_change_amounts_ko():
    return [_CASH_VALUE + 1, -_FUND1_VALUE - 1]


@pytest.mark.parametrize("amount",
                         get_test_handle_cash_amount_change_amounts_ko())
def test_handle_cash_amount_change_ko(amount, monkeypatch):
    portfolio = DriveWealthPortfolio(_PORTFOLIO)
    drivewealth_repository = DriveWealthRepository(None)
    api = DriveWealthApi()

    def mock_get_portfolio_status(_portfolio):
        assert _portfolio == portfolio
        return _PORTFOLIO_STATUS

    monkeypatch.setattr(api, "get_portfolio_status", mock_get_portfolio_status)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)

    service = DriveWealthProviderCollection(drivewealth_repository, api)
    fund = DriveWealthFund()
    monkeypatch.setattr(fund, "ref_id", _FUND1_ID)

    with pytest.raises(InsufficientFundsException) as error_info:
        service._handle_cash_amount_change(amount, portfolio, fund)
        assert error_info.__class__ == InsufficientFundsException


def test_update_portfolio(monkeypatch):
    portfolio = DriveWealthPortfolio()
    drivewealth_repository = DriveWealthRepository(None)
    api = DriveWealthApi()

    def mock_get_portfolio_status(_portfolio):
        assert _portfolio == portfolio
        return _PORTFOLIO_STATUS

    monkeypatch.setattr(api, "get_portfolio_status", mock_get_portfolio_status)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)

    service = DriveWealthProviderCollection(drivewealth_repository, api)

    portfolio_status = service._get_portfolio_status(portfolio)

    assert portfolio_status
    assert portfolio_status.cash_value == _CASH_VALUE
    assert portfolio_status.get_fund_value(_FUND1_ID) == _FUND1_VALUE
    assert portfolio_status.get_fund_value(_FUND2_ID) == _FUND2_VALUE
