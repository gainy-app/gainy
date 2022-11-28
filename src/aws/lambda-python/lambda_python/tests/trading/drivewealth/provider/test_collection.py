from decimal import Decimal

from gainy.tests.mocks.repository_mocks import mock_noop, mock_find
from gainy.tests.mocks.trading.drivewealth.api_mocks import FUND1_ID, PORTFOLIO_STATUS, PORTFOLIO, USER_ID
from gainy.trading.models import TradingCollectionVersion
from trading.drivewealth.api import DriveWealthApi
from trading.drivewealth.repository import DriveWealthRepository
from trading.drivewealth.provider.collection import DriveWealthProviderCollection

from gainy.trading.drivewealth.models import DriveWealthUser, DriveWealthAccount, DriveWealthPortfolio, DriveWealthFund, \
    DriveWealthInstrument, DriveWealthPortfolioStatus

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
    trading_collection_version_id = 3
    trading_account_id = 3

    user = DriveWealthUser()
    monkeypatch.setattr(user, "ref_id", USER_ID)

    portfolio = DriveWealthPortfolio()
    portfolio.set_from_response(PORTFOLIO)

    portfolio_status = DriveWealthPortfolioStatus()
    portfolio_status.set_from_response(PORTFOLIO_STATUS)

    fund = DriveWealthFund()
    monkeypatch.setattr(fund, "ref_id", FUND1_ID)
    monkeypatch.setattr(fund, "profile_id", profile_id)
    monkeypatch.setattr(fund, "trading_collection_version_id",
                        trading_collection_version_id)

    trading_collection_version = TradingCollectionVersion()
    monkeypatch.setattr(trading_collection_version, "trading_account_id",
                        trading_account_id)

    def mock_get_user(_profile_id):
        assert _profile_id == profile_id
        return user

    def mock_get_profile_portfolio(_profile_id, _trading_account_id):
        assert _profile_id == profile_id
        assert _trading_account_id == trading_account_id
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
    monkeypatch.setattr(
        drivewealth_repository, "find_one",
        mock_find([(TradingCollectionVersion, {
            "id": trading_collection_version_id
        }, trading_collection_version)]))

    def mock_sync_portfolio_status(_portfolio):
        assert _portfolio == portfolio
        return portfolio_status

    service = DriveWealthProviderCollection(drivewealth_repository, None)
    monkeypatch.setattr(service, "sync_portfolio_status",
                        mock_sync_portfolio_status)
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
