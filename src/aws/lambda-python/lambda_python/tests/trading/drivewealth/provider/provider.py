from decimal import Decimal

import pytest

from tests.repository_mocks import mock_find_one, mock_persist, mock_noop
from tests.trading.drivewealth.api_mocks import mock_get_user_accounts, mock_get_account_money, \
    mock_get_account_positions, mock_create_deposit, mock_create_redemption
from trading.models import TradingMoneyFlow, TradingAccount
from trading.drivewealth.api import DriveWealthApi
from trading.drivewealth.models import DriveWealthAccount, DriveWealthBankAccount, DriveWealthDeposit, \
    DriveWealthRedemption, DriveWealthUser, DriveWealthAccountMoney, DriveWealthAccountPositions
from trading.drivewealth.repository import DriveWealthRepository
from trading.drivewealth.provider.provider import DriveWealthProvider


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
        mock_find_one([
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


def test_sync_trading_accounts(monkeypatch):
    cash_balance_list = 10
    cash_available_for_trade_list = 20
    cash_available_for_withdrawal_list = 30
    cash_balance = 1
    cash_available_for_trade = 2
    cash_available_for_withdrawal = 3
    equity_value = 4
    account_ref_id = "account_ref_id"
    user_ref_id = "user_ref_id"
    trading_account_id = "trading_account_id"

    trading_account = TradingAccount()

    account = DriveWealthAccount()
    monkeypatch.setattr(account, "trading_account_id", trading_account_id)

    user = DriveWealthUser()
    monkeypatch.setattr(user, "ref_id", user_ref_id)

    persisted_objects = {}
    drivewealth_repository = DriveWealthRepository(None)
    monkeypatch.setattr(drivewealth_repository, "persist",
                        mock_persist(persisted_objects))
    monkeypatch.setattr(
        drivewealth_repository, "find_one",
        mock_find_one([
            (DriveWealthAccount, {
                "ref_id": account_ref_id
            }, account),
            (TradingAccount, {
                "id": trading_account_id
            }, trading_account),
        ]))

    api = DriveWealthApi(drivewealth_repository)
    monkeypatch.setattr(
        api, "get_user_accounts",
        mock_get_user_accounts(
            user_ref_id,
            account_ref_id,
            cash_balance=cash_balance_list,
            cash_available_for_trade=cash_available_for_trade_list,
            cash_available_for_withdrawal=cash_available_for_withdrawal_list))
    monkeypatch.setattr(
        api, "get_account_money",
        mock_get_account_money(
            account_ref_id,
            cash_balance=cash_balance,
            cash_available_for_trade=cash_available_for_trade,
            cash_available_for_withdrawal=cash_available_for_withdrawal))
    monkeypatch.setattr(
        api, "get_account_positions",
        mock_get_account_positions(account_ref_id, equity_value=equity_value))

    service = DriveWealthProvider(drivewealth_repository, api, None)
    service._sync_trading_accounts(user)

    assert DriveWealthAccount in persisted_objects
    assert DriveWealthAccountMoney in persisted_objects
    assert DriveWealthAccountPositions in persisted_objects
    assert TradingAccount in persisted_objects

    assert persisted_objects[DriveWealthAccount][0] == account
    assert account.ref_id == account_ref_id
    assert account.cash_balance == cash_balance_list
    assert account.cash_available_for_trade == cash_available_for_trade_list
    assert account.cash_available_for_withdrawal == cash_available_for_withdrawal_list

    account_money: DriveWealthAccountMoney = persisted_objects[
        DriveWealthAccountMoney][0]
    assert account_money.drivewealth_account_id == account_ref_id
    assert account_money.cash_balance == cash_balance
    assert account_money.cash_available_for_trade == cash_available_for_trade
    assert account_money.cash_available_for_withdrawal == cash_available_for_withdrawal

    account_positions: DriveWealthAccountPositions = persisted_objects[
        DriveWealthAccountPositions][0]
    assert account_positions.drivewealth_account_id == account_ref_id
    assert account_positions.equity_value == equity_value

    assert persisted_objects[TradingAccount][0] == trading_account
    assert trading_account.cash_balance == cash_balance
    assert trading_account.cash_available_for_trade == cash_available_for_trade
    assert trading_account.cash_available_for_withdrawal == cash_available_for_withdrawal
    assert trading_account.equity_value == equity_value
