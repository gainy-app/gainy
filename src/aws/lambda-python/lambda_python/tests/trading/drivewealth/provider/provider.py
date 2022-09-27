from decimal import Decimal

import pytest

from gainy.tests.mocks.repository_mocks import mock_find, mock_persist, mock_noop
from tests.trading.drivewealth.api_mocks import mock_create_deposit, mock_create_redemption, mock_get_deposit
from trading.models import TradingMoneyFlow
from trading.drivewealth.models import DriveWealthBankAccount, DriveWealthDeposit, DriveWealthRedemption
from trading.drivewealth.api import DriveWealthApi
from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.repository import DriveWealthRepository

from gainy.trading.drivewealth.models import DriveWealthAccount


def get_test_transfer_money_amounts():
    return [3, -3]


@pytest.mark.parametrize("amount", get_test_transfer_money_amounts())
def test_transfer_money(monkeypatch, amount):
    trading_account_id = 1
    funding_account_id = 2
    amount = Decimal(amount)
    money_flow_id = 4
    profile_id = 5
    account_ref_id = "account_ref_id"
    bank_account_ref_id = "bank_account_ref_id"
    transfer_ref_id = "transfer_ref_id"
    status = "status"

    account = DriveWealthAccount()
    monkeypatch.setattr(account, "ref_id", account_ref_id)
    bank_account = DriveWealthBankAccount()
    monkeypatch.setattr(bank_account, "ref_id", bank_account_ref_id)

    drivewealth_repository = DriveWealthRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(drivewealth_repository, "persist",
                        mock_persist(persisted_objects))
    monkeypatch.setattr(
        drivewealth_repository, "find_one",
        mock_find([
            (DriveWealthAccount, {
                "trading_account_id": trading_account_id
            }, account),
            (DriveWealthBankAccount, {
                "funding_account_id": funding_account_id
            }, bank_account),
        ]))

    api = DriveWealthApi(drivewealth_repository)
    monkeypatch.setattr(
        api, "create_deposit",
        mock_create_deposit(amount,
                            account,
                            bank_account,
                            ref_id=transfer_ref_id,
                            status=status))
    monkeypatch.setattr(
        api, "create_redemption",
        mock_create_redemption(amount,
                               account,
                               bank_account,
                               ref_id=transfer_ref_id,
                               status=status))

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
    assert entity.__class__ in persisted_objects

    assert entity.ref_id == transfer_ref_id
    assert entity.status == status
    assert entity.trading_account_ref_id == account_ref_id
    assert entity.bank_account_ref_id == bank_account_ref_id
    assert entity.money_flow_id == money_flow_id


def test_sync_deposit(monkeypatch):
    deposit_ref_id = "deposit_ref_id"
    money_flow_id = 4
    status = "status"

    deposit = DriveWealthDeposit()
    monkeypatch.setattr(deposit, "money_flow_id", money_flow_id)
    money_flow = TradingMoneyFlow()

    drivewealth_repository = DriveWealthRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(drivewealth_repository, "persist",
                        mock_persist(persisted_objects))
    monkeypatch.setattr(
        drivewealth_repository, "find_one",
        mock_find([
            (DriveWealthDeposit, {
                "ref_id": deposit_ref_id
            }, deposit),
            (TradingMoneyFlow, {
                "id": money_flow_id
            }, money_flow),
        ]))

    api = DriveWealthApi(drivewealth_repository)
    monkeypatch.setattr(api, "get_deposit",
                        mock_get_deposit(deposit_ref_id, status=status))

    service = DriveWealthProvider(drivewealth_repository, api, None)
    service.sync_deposit(deposit_ref_id=deposit_ref_id, fetch_info=True)

    assert deposit.__class__ in persisted_objects
    assert deposit.ref_id == deposit_ref_id
    assert deposit.status == status
    assert deposit.money_flow_id == money_flow_id

    assert money_flow.__class__ in persisted_objects
    assert money_flow.status == status
