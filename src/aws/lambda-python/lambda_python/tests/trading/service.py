from trading import TradingRepository, TradingService
from trading.drivewealth.models import DriveWealthPortfolioStatusFundHolding
from trading.drivewealth.provider.collection import DriveWealthProviderCollection


def mock_noop(*args, **kwargs):
    pass


def test_get_actual_collection_holdings(monkeypatch):
    profile_id = 1
    collection_id = 2

    expected_holdings = {
        "instrumentID": "5b85fabb-d57c-44e6-a7f6-a3efc760226c",
        "symbol": "TSLA",
        "target": 0.55,
        "actual": 0.6823,
        "openQty": 62.5213,
        "value": 22928.9
    }

    drivewealth_holdings = [
        DriveWealthPortfolioStatusFundHolding(expected_holdings)
    ]

    def mock_get_actual_collection_holdings(_profile_id, _collection_id):
        assert _profile_id == profile_id
        assert _collection_id == collection_id
        return drivewealth_holdings

    drivewealth_provider = DriveWealthProviderCollection(None, None)
    monkeypatch.setattr(drivewealth_provider, "get_actual_collection_holdings",
                        mock_get_actual_collection_holdings)

    trading_repository = TradingRepository(None)
    service = TradingService(None, trading_repository, drivewealth_provider,
                             None)
    holdings = service.get_actual_collection_holdings(profile_id,
                                                      collection_id)

    assert holdings[0].symbol == expected_holdings["symbol"]
    assert holdings[0].target_weight == expected_holdings["target"]
    assert holdings[0].actual_weight == expected_holdings["actual"]
    assert holdings[0].value == expected_holdings["value"]
