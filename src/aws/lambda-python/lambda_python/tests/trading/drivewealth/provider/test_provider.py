import datetime
from decimal import Decimal

import pytest

from gainy.data_access.models import BaseModel
from gainy.tests.mocks.repository_mocks import mock_find, mock_persist, mock_noop, mock_record_calls
from gainy.trading.models import TradingMoneyFlowStatus
from services.notification import NotificationService
from tests.trading.drivewealth.api_mocks import mock_create_deposit, mock_create_redemption, mock_get_deposit, \
    mock_get_redemption
from trading.models import TradingMoneyFlow, TradingStatement
from trading.drivewealth.models import DriveWealthBankAccount, DriveWealthDeposit, DriveWealthRedemption, \
    DriveWealthStatement, DriveWealthOrder
from trading.drivewealth.api import DriveWealthApi
from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.repository import DriveWealthRepository

from gainy.trading.drivewealth.models import DriveWealthAccount, DriveWealthInstrument, DriveWealthInstrumentStatus, \
    DriveWealthPortfolio


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

    service = DriveWealthProvider(drivewealth_repository, api, None, None,
                                  None)
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
    account_ref_id = "account_ref_id"
    money_flow_id = 4
    status = 'Successful'

    account = DriveWealthAccount()

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
            (DriveWealthAccount, {
                "ref_id": account_ref_id,
            }, account),
            (TradingMoneyFlow, {
                "id": money_flow_id
            }, money_flow),
        ]))

    api = DriveWealthApi(drivewealth_repository)
    monkeypatch.setattr(
        api, "get_deposit",
        mock_get_deposit(deposit_ref_id, account_ref_id, status=status))

    service = DriveWealthProvider(drivewealth_repository, api, None, None,
                                  None)
    service.sync_deposit(deposit_ref_id=deposit_ref_id)

    assert deposit.__class__ in persisted_objects
    assert deposit.ref_id == deposit_ref_id
    assert deposit.status == status
    assert deposit.money_flow_id == money_flow_id

    assert money_flow.__class__ in persisted_objects
    assert money_flow.status == TradingMoneyFlowStatus.SUCCESS


def test_sync_redemption(monkeypatch):
    redemption_ref_id = "redemption_ref_id"
    account_ref_id = "account_ref_id"
    money_flow_id = 4
    status = 'Successful'

    account = DriveWealthAccount()

    redemption = DriveWealthRedemption()
    monkeypatch.setattr(redemption, "money_flow_id", money_flow_id)
    money_flow = TradingMoneyFlow()

    drivewealth_repository = DriveWealthRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(drivewealth_repository, "persist",
                        mock_persist(persisted_objects))
    monkeypatch.setattr(
        drivewealth_repository, "find_one",
        mock_find([
            (DriveWealthRedemption, {
                "ref_id": redemption_ref_id
            }, redemption),
            (DriveWealthAccount, {
                "ref_id": account_ref_id,
            }, account),
            (TradingMoneyFlow, {
                "id": money_flow_id
            }, money_flow),
        ]))

    api = DriveWealthApi(drivewealth_repository)
    monkeypatch.setattr(
        api, "get_redemption",
        mock_get_redemption(redemption_ref_id, account_ref_id, status=status))

    service = DriveWealthProvider(drivewealth_repository, api, None, None,
                                  None)
    service.sync_redemption(redemption_ref_id=redemption_ref_id)

    assert redemption.__class__ in persisted_objects
    assert redemption.ref_id == redemption_ref_id
    assert redemption.status == status
    assert redemption.money_flow_id == money_flow_id

    assert money_flow.__class__ in persisted_objects
    assert money_flow.status == TradingMoneyFlowStatus.SUCCESS


def test_create_trading_statements(monkeypatch):
    profile_id = 1
    trading_statement_id1 = 1
    trading_statement_id2 = 2

    entity1 = DriveWealthStatement()
    entity1.type = "type1"
    entity1.display_name = "display_name1"
    entity2 = DriveWealthStatement()
    entity2.type = "type2"
    entity2.display_name = "display_name2"
    entity2.trading_statement_id = trading_statement_id2

    entities = [entity1, entity2]

    repository = DriveWealthRepository(None)
    persisted_objects = {}

    def custom_mock_persist(persisted_objects):
        _mock = mock_persist(persisted_objects)

        def mock(entities):
            _mock(entities)

            if isinstance(entities, BaseModel):
                entities = [entities]

            for entity in entities:
                if isinstance(entity, TradingStatement):
                    entity.id = trading_statement_id1

        return mock

    monkeypatch.setattr(repository, "persist",
                        custom_mock_persist(persisted_objects))

    provider = DriveWealthProvider(repository, None, None, None, None)
    provider.create_trading_statements(entities, profile_id)

    assert entity1 in persisted_objects[DriveWealthStatement]
    assert entity2 not in persisted_objects[DriveWealthStatement]
    assert len(persisted_objects[TradingStatement]) == 1
    trading_statement = persisted_objects[TradingStatement][0]
    assert trading_statement.profile_id == profile_id
    assert trading_statement.type == entity1.type
    assert trading_statement.display_name == entity1.display_name
    assert entity1.trading_statement_id == trading_statement_id1


def test_download_statement(monkeypatch):
    trading_statement_id = 1
    url = "url"

    dw_statement = DriveWealthStatement()
    statement = TradingStatement()
    statement.id = trading_statement_id

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, "find_one",
        mock_find([(DriveWealthStatement, {
            "trading_statement_id": statement.id
        }, dw_statement)]))
    api = DriveWealthApi(None)

    def mock_get_statement_url(_dw_statement):
        assert _dw_statement == dw_statement
        return url

    monkeypatch.setattr(api, "get_statement_url", mock_get_statement_url)

    provider = DriveWealthProvider(repository, api, None, None, None)
    assert url == provider.download_statement(statement)


def test_handle_instrument_status_change(monkeypatch):
    symbol = "symbol"
    status = DriveWealthInstrumentStatus.ACTIVE
    new_status = "new_status"

    instrument = DriveWealthInstrument()
    monkeypatch.setattr(instrument, "status", status)
    monkeypatch.setattr(instrument, "symbol", symbol)

    repository = DriveWealthRepository(None)

    def mock_symbol_is_in_collection(_symbol):
        assert symbol == _symbol
        return True

    monkeypatch.setattr(repository, "symbol_is_in_collection",
                        mock_symbol_is_in_collection)

    notification_service = NotificationService(None)
    calls = []
    monkeypatch.setattr(notification_service,
                        "notify_dw_instrument_status_changed",
                        mock_record_calls(calls))

    provider = DriveWealthProvider(repository, None, None, None,
                                   notification_service)
    provider.handle_instrument_status_change(instrument, new_status)

    assert (symbol, status, new_status) in [args for args, kwargs in calls]


def test_handle_order(monkeypatch):
    order_executed_at = datetime.datetime.now()
    last_order_executed_at = order_executed_at - datetime.timedelta(seconds=1)
    account_id = 1
    account_ref_id = "account_ref_id"

    order = DriveWealthOrder()
    order.last_executed_at = order_executed_at
    order.account_id = account_id

    account = DriveWealthAccount()
    account.ref_id = account_ref_id

    portfolio = DriveWealthPortfolio()
    portfolio.last_order_executed_at = last_order_executed_at

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, "find_one",
        mock_find([
            (DriveWealthAccount, {
                "ref_id": account_id
            }, account),
            (DriveWealthPortfolio, {
                "drivewealth_account_id": account_ref_id
            }, portfolio),
        ]))
    persisted_objects = {}
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))

    provider = DriveWealthProvider(repository, None, None, None, None)
    provider.handle_order(order)

    assert DriveWealthPortfolio in persisted_objects
    assert portfolio in persisted_objects[DriveWealthPortfolio]
    assert portfolio.last_order_executed_at == order_executed_at
