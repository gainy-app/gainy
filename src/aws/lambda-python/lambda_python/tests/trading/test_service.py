from trading.models import CollectionStatus
from trading.service import TradingService
from trading.repository import TradingRepository
from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.models import DriveWealthPortfolioStatusFundHolding, DriveWealthPortfolioStatusHolding


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

    collection_status = CollectionStatus()
    collection_status.holdings = [
        i.get_collection_holding_status() for i in drivewealth_holdings
    ]

    def mock_get_actual_collection_data(_profile_id, _collection_id):
        assert _profile_id == profile_id
        assert _collection_id == collection_id
        return collection_status

    drivewealth_provider = DriveWealthProvider(None, None, None)
    monkeypatch.setattr(drivewealth_provider, "get_actual_collection_data",
                        mock_get_actual_collection_data)

    trading_repository = TradingRepository(None)
    service = TradingService(None, trading_repository, drivewealth_provider,
                             None)
    holdings = service.get_actual_collection_holdings(profile_id,
                                                      collection_id)

    assert holdings[0].symbol == expected_holdings["symbol"]
    assert holdings[0].target_weight == expected_holdings["target"]
    assert holdings[0].actual_weight == expected_holdings["actual"]
    assert holdings[0].value == expected_holdings["value"]
