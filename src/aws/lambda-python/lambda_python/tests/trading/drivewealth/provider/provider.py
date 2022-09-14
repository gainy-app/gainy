from decimal import Decimal

from trading.models import TradingMoneyFlow
from trading.drivewealth.api import DriveWealthApi
from trading.drivewealth.models import DriveWealthAccount, DriveWealthBankAccount, DriveWealthDeposit, \
    DriveWealthRedemption
from trading.drivewealth.repository import DriveWealthRepository
from trading.drivewealth.provider.provider import DriveWealthProvider


def mock_noop(*args, **kwargs):
    pass


def test_transfer_money(monkeypatch):
    trading_account_id = 1
    funding_account_id = 2
    amount = Decimal(3)
    money_flow_id = 4
    profile_id = 5
    account_ref_id = "account_ref_id"
    bank_account_ref_id = "bank_account_ref_id"

    account = DriveWealthAccount()
    monkeypatch.setattr(account, "ref_id", account_ref_id)
    bank_account = DriveWealthBankAccount()
    monkeypatch.setattr(bank_account, "ref_id", bank_account_ref_id)

    def mock_find_one(cls, fltr):
        if cls == DriveWealthAccount:
            assert fltr == {"trading_account_id": trading_account_id}
            return account
        if cls == DriveWealthBankAccount:
            assert fltr == {"funding_account_id": funding_account_id}
            return bank_account
        raise Exception('unknown class %s', cls)

    drivewealth_repository = DriveWealthRepository(None)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)
    monkeypatch.setattr(drivewealth_repository, "find_one", mock_find_one)

    api = DriveWealthApi()
    data = {
        "id": "DWZR000...",
        "status": {
            "id": 1,
            "message": "Pending",
            "updated": "2017-11-10T14:43:26.497Z"
        },
        "created": "2017-11-10T14:43:26.497Z"
    }

    def mock_create(_amount, _account, _bank_account):
        assert _amount == amount
        assert _account == account
        assert _bank_account == bank_account
        return data

    monkeypatch.setattr(api, "create_deposit", mock_create)
    monkeypatch.setattr(api, "create_redemption", mock_create)

    money_flow = TradingMoneyFlow()
    money_flow.profile_id = profile_id
    money_flow.amount = amount
    money_flow.trading_account_id = trading_account_id
    money_flow.funding_account_id = funding_account_id
    monkeypatch.setattr(money_flow, "id", money_flow_id)
    monkeypatch.setattr(money_flow, "profile_id", profile_id)

    service = DriveWealthProvider(drivewealth_repository, api, None)
    monkeypatch.setattr(service, "_on_money_transfer", mock_noop)
    entity = service.transfer_money(money_flow, amount, trading_account_id,
                                    funding_account_id)

    if amount > 0:
        assert entity.__class__ == DriveWealthDeposit
    else:
        assert entity.__class__ == DriveWealthRedemption

    assert entity.ref_id == data["id"]
    assert entity.status == data["status"]["message"]
    assert entity.trading_account_ref_id == account_ref_id
    assert entity.bank_account_ref_id == bank_account_ref_id
    assert entity.money_flow_id == money_flow_id
    assert entity.data == data
