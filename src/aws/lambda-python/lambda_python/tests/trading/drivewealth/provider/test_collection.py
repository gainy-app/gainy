from decimal import Decimal

import pytest

from gainy.tests.mocks.repository_mocks import mock_noop, mock_persist, mock_find
from gainy.tests.mocks.trading.drivewealth.api_mocks import CASH_TARGET_WEIGHT, FUND1_ID, FUND1_TARGET_WEIGHT, FUND2_ID, \
    FUND2_TARGET_WEIGHT, PORTFOLIO_STATUS, CASH_VALUE, FUND1_VALUE
from trading.exceptions import InsufficientFundsException
from trading.drivewealth.api import DriveWealthApi
from trading.drivewealth.repository import DriveWealthRepository
from trading.drivewealth.models import DriveWealthInstrumentStatus, DriveWealthInstrument
from trading.drivewealth.provider.collection import DriveWealthProviderCollection
from trading.models import TradingCollectionVersion

from gainy.trading.drivewealth.models import DriveWealthUser, DriveWealthAccount, DriveWealthPortfolio, DriveWealthFund

_USER_ID = "41dde78c-e31b-43e5-9418-44ae08098738"

_PORTFOLIO_REF_ID = "portfolio_24338197-62da-4ac8-a0c9-3204e396f9c7"
_PORTFOLIO = {
    "id":
    _PORTFOLIO_REF_ID,
    "userID":
    _USER_ID,
    "holdings": [{
        "instrumentID": None,
        "type": "CASH_RESERVE",
        "target": CASH_TARGET_WEIGHT,
    }, {
        "instrumentID": FUND1_ID,
        "type": "FUND",
        "target": FUND1_TARGET_WEIGHT,
    }, {
        "instrumentID": FUND2_ID,
        "type": "FUND",
        "target": FUND2_TARGET_WEIGHT,
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


def _mock_get_instrument(monkeypatch, service):
    instrument_B = DriveWealthInstrument()
    monkeypatch.setattr(instrument_B, 'ref_id', 'B')
    instrument_C = DriveWealthInstrument()
    monkeypatch.setattr(instrument_C, 'ref_id', 'C')

    def mock_get_instrument(symbol):
        instruments = {
            "symbol_B": instrument_B,
            "symbol_C": instrument_C,
        }
        return instruments[symbol]

    monkeypatch.setattr(service, "_get_instrument", mock_get_instrument)


def test_get_actual_collection_data(monkeypatch):
    profile_id = 1
    collection_id = 2

    user = DriveWealthUser()
    monkeypatch.setattr(user, "ref_id", _USER_ID)

    portfolio = DriveWealthPortfolio(_PORTFOLIO)
    fund = DriveWealthFund()
    monkeypatch.setattr(fund, "ref_id", FUND1_ID)
    monkeypatch.setattr(fund, "profile_id", profile_id)

    def mock_get_user(_profile_id):
        assert _profile_id == profile_id
        return user

    def mock_get_profile_portfolio(_profile_id):
        assert _profile_id == profile_id
        return portfolio

    def mock_get_profile_fund(_profile_id, _collection_id):
        assert _profile_id == profile_id
        assert _collection_id == collection_id
        return fund

    drivewealth_repository = DriveWealthRepository(None)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)
    monkeypatch.setattr(drivewealth_repository, "get_user", mock_get_user)
    monkeypatch.setattr(drivewealth_repository, "get_profile_portfolio",
                        mock_get_profile_portfolio)
    monkeypatch.setattr(drivewealth_repository, "get_profile_fund",
                        mock_get_profile_fund)

    api = DriveWealthApi(None)

    def mock_get_portfolio_status(_portfolio):
        assert _portfolio == portfolio
        return PORTFOLIO_STATUS

    monkeypatch.setattr(api, "get_portfolio_status", mock_get_portfolio_status)

    service = DriveWealthProviderCollection(drivewealth_repository, api)
    holdings = service.get_actual_collection_data(profile_id,
                                                  collection_id).holdings

    expected_holdings = PORTFOLIO_STATUS["holdings"][1]["holdings"]
    for i in range(2):
        assert holdings[i].symbol == expected_holdings[i]["symbol"]
        assert holdings[i].target_weight == expected_holdings[i]["target"]
        assert holdings[i].actual_weight == expected_holdings[i]["actual"]
        assert holdings[i].value == expected_holdings[i]["value"]


def test_create_autopilot_run(monkeypatch):
    collection_version_id = 1
    collection_version = TradingCollectionVersion()
    monkeypatch.setattr(collection_version, "id", collection_version_id)

    account = DriveWealthAccount()
    monkeypatch.setattr(account, "ref_id", _ACCOUNT_ID)

    drivewealth_repository = DriveWealthRepository(None)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)

    api = DriveWealthApi(None)
    data = {
        "id": "ria_rebalance_804d3f5f-a352-4320-992f-c81bf773a739",
        "created": "2019-05-15T19:51:09.147Z",
        "status": "REBALANCE_NOT_STARTED",
        "riaID": "2ffe9863-dd93-4964-87d1-bda90472984f"
    }

    def mock_create_autopilot_run(_account_ref_ids):
        assert _account_ref_ids == [_ACCOUNT_ID]
        return data

    monkeypatch.setattr(api, "create_autopilot_run", mock_create_autopilot_run)

    service = DriveWealthProviderCollection(drivewealth_repository, api)
    autopilot_run = service._create_autopilot_run(account, collection_version)

    assert autopilot_run.ref_id == data["id"]
    assert autopilot_run.status == data["status"]
    assert autopilot_run.account_id == _ACCOUNT_ID
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

    def mock_get_profile_portfolio(_profile_id):
        assert _profile_id == profile_id
        return None

    drivewealth_repository = DriveWealthRepository(None)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)
    monkeypatch.setattr(drivewealth_repository, "get_user", mock_get_user)
    monkeypatch.setattr(drivewealth_repository, "get_profile_portfolio",
                        mock_get_profile_portfolio)

    api = DriveWealthApi(None)

    def mock_create_portfolio(_name, _client_portfolio_id, _description):
        assert _client_portfolio_id == profile_id
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
    assert portfolio.get_fund_weight(FUND1_ID) == FUND1_TARGET_WEIGHT


def get_test_upsert_fund_fund_exists():
    return [False, True]


@pytest.mark.parametrize("fund_exists", get_test_upsert_fund_fund_exists())
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

    def mock_get_profile_fund(_profile_id, _collection_id):
        assert _profile_id == profile_id
        assert _collection_id == collection_id
        return fund if fund_exists else None

    drivewealth_repository = DriveWealthRepository(None)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)
    monkeypatch.setattr(drivewealth_repository, "get_user", mock_get_user)
    monkeypatch.setattr(drivewealth_repository, "get_profile_fund",
                        mock_get_profile_fund)

    data = {"id": fund_ref_id, "userID": _USER_ID, "holdings": _FUND_HOLDINGS}

    new_fund_holdings = [
        {
            "instrumentID": "B",
            "target": Decimal(0.3),
        },
        {
            "instrumentID": "C",
            "target": Decimal(0.7),
        },
    ]

    api = DriveWealthApi(None)
    if fund_exists:

        def mock_update_fund(_fund):
            assert _fund == fund
            assert _fund.holdings == new_fund_holdings
            return data

        monkeypatch.setattr(api, "update_fund", mock_update_fund)
    else:

        def mock_create_fund(_name, _client_fund_id, _description,
                             _new_fund_holdings):
            assert _client_fund_id == f"{profile_id}_{collection_id}"
            assert _new_fund_holdings == new_fund_holdings
            return data

        monkeypatch.setattr(api, "create_fund", mock_create_fund)

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
    _mock_get_instrument(monkeypatch, service)
    fund = service._upsert_fund(profile_id, collection_version)

    if not fund_exists:
        assert fund.ref_id == fund_ref_id
    assert fund.collection_id == collection_id
    assert fund.trading_collection_version_id == trading_collection_version_id
    assert fund.weights == weights
    assert fund.data == data


def test_generate_new_fund_holdings(monkeypatch):
    drivewealth_repository = DriveWealthRepository(None)
    api = DriveWealthApi(None)

    service = DriveWealthProviderCollection(drivewealth_repository, api)
    fund = DriveWealthFund()
    monkeypatch.setattr(DriveWealthFund, "holdings", _FUND_HOLDINGS)

    _mock_get_instrument(monkeypatch, service)

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
    yield from [100, CASH_VALUE - 100, CASH_VALUE]
    yield 0
    yield from [-100, -CASH_VALUE, -FUND1_VALUE + 100, -FUND1_VALUE]


@pytest.mark.parametrize("amount",
                         get_test_handle_cash_amount_change_amounts_ok())
def test_handle_cash_amount_change_ok(amount, monkeypatch):
    amount = Decimal(amount)
    portfolio = DriveWealthPortfolio()
    portfolio.set_from_response(_PORTFOLIO)
    drivewealth_repository = DriveWealthRepository(None)
    api = DriveWealthApi(None)

    def mock_get_portfolio_status(_portfolio):
        assert _portfolio == portfolio
        return PORTFOLIO_STATUS

    monkeypatch.setattr(api, "get_portfolio_status", mock_get_portfolio_status)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)

    service = DriveWealthProviderCollection(drivewealth_repository, api)
    fund = DriveWealthFund()
    monkeypatch.setattr(fund, "ref_id", FUND1_ID)

    assert abs(portfolio.cash_target_weight - CASH_TARGET_WEIGHT) < 1e-3
    assert abs(portfolio.get_fund_weight(FUND1_ID) -
               FUND1_TARGET_WEIGHT) < 1e-3

    service._handle_cash_amount_change(amount, portfolio, fund)

    if amount:
        assert abs(portfolio.cash_target_weight -
                   (CASH_VALUE - amount) / PORTFOLIO_STATUS['equity']) < 1e-3
        assert abs(
            portfolio.get_fund_weight(FUND1_ID) -
            (FUND1_VALUE + amount) / PORTFOLIO_STATUS['equity']) < 1e-3
    else:
        assert abs(portfolio.cash_target_weight - CASH_TARGET_WEIGHT) < 1e-3
        assert abs(portfolio.get_fund_weight(FUND1_ID) -
                   FUND1_TARGET_WEIGHT) < 1e-3


def get_test_handle_cash_amount_change_amounts_ko():
    return [CASH_VALUE + 1, -FUND1_VALUE - 1]


@pytest.mark.parametrize("amount",
                         get_test_handle_cash_amount_change_amounts_ko())
def test_handle_cash_amount_change_ko(amount, monkeypatch):
    portfolio = DriveWealthPortfolio(_PORTFOLIO)
    drivewealth_repository = DriveWealthRepository(None)
    api = DriveWealthApi(None)

    def mock_get_portfolio_status(_portfolio):
        assert _portfolio == portfolio
        return PORTFOLIO_STATUS

    monkeypatch.setattr(api, "get_portfolio_status", mock_get_portfolio_status)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)

    service = DriveWealthProviderCollection(drivewealth_repository, api)
    fund = DriveWealthFund()
    monkeypatch.setattr(fund, "ref_id", FUND1_ID)

    with pytest.raises(InsufficientFundsException) as error_info:
        service._handle_cash_amount_change(amount, portfolio, fund)
        assert error_info.__class__ == InsufficientFundsException


def get_test_upsert_fund_instrument_exists():
    return [False, True]


@pytest.mark.parametrize("instrument_exists",
                         get_test_upsert_fund_instrument_exists())
def test_get_instrument(instrument_exists, monkeypatch):
    _symbol = "symbol"
    drivewealth_repository = DriveWealthRepository(None)

    service = DriveWealthProviderCollection(drivewealth_repository, None)
    instrument = DriveWealthInstrument()

    if instrument_exists:
        monkeypatch.setattr(
            drivewealth_repository, 'find_one',
            mock_find([
                (DriveWealthInstrument, {
                    "symbol": _symbol,
                    "status": DriveWealthInstrumentStatus.ACTIVE
                }, instrument),
            ]))
    else:
        monkeypatch.setattr(
            drivewealth_repository, 'find_one',
            mock_find([
                (DriveWealthInstrument, {
                    "symbol": _symbol,
                    "status": DriveWealthInstrumentStatus.ACTIVE
                }, None),
            ]))

        def mock_sync_instrument(symbol):
            assert symbol == _symbol
            return instrument

        monkeypatch.setattr(service, 'sync_instrument', mock_sync_instrument)

    assert service._get_instrument(_symbol) == instrument


def test_sync_instrument(monkeypatch):
    instrument_ref_id = "instrument_ref_id"
    instrument_symbol = "symbol"
    instrument_status = str(DriveWealthInstrumentStatus.ACTIVE)
    instrument_data = {
        "instrumentID": instrument_ref_id,
        "symbol": instrument_symbol,
        "status": instrument_status,
    }

    drivewealth_repository = DriveWealthRepository(None)
    api = DriveWealthApi(None)

    def mock_get_instrument_details(ref_id: str = None, symbol: str = None):
        assert ref_id == instrument_ref_id
        assert symbol == instrument_symbol
        return instrument_data

    persisted_objects = {}
    monkeypatch.setattr(api, "get_instrument_details",
                        mock_get_instrument_details)
    monkeypatch.setattr(drivewealth_repository, "persist",
                        mock_persist(persisted_objects))

    service = DriveWealthProviderCollection(drivewealth_repository, api)

    instrument = service.sync_instrument(ref_id=instrument_ref_id,
                                         symbol=instrument_symbol)

    assert DriveWealthInstrument in persisted_objects
    assert persisted_objects[DriveWealthInstrument]

    assert instrument in persisted_objects[DriveWealthInstrument]
    assert instrument.ref_id == instrument_ref_id
    assert instrument.symbol == instrument_symbol
    assert instrument.status == instrument_status
    assert instrument.data == instrument_data
