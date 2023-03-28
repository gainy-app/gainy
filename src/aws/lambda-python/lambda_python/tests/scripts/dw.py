from _decimal import Decimal

from gainy.context_container import ContextContainer
from gainy.trading.drivewealth.models import DriveWealthPortfolio, DriveWealthFund, DriveWealthAccount, DriveWealthUser
from gainy.trading.drivewealth.provider.rebalance_helper import DriveWealthProviderRebalanceHelper
from gainy.trading.models import TradingCollectionVersion, TradingAccount

profile_id = None
account_ref_id = None
portfolio_ref_id = None
fund_ref_id = None
target_amount_delta = Decimal(None)

with ContextContainer() as context_container:
    provider = context_container.drivewealth_provider
    api = context_container.drivewealth_api
    trading_repository = context_container.trading_repository

    account_data = api.get_account(account_ref_id)
    account = DriveWealthAccount()
    account.set_from_response(account_data)
    if not trading_repository.find_one(
            DriveWealthUser, {"ref_id": account.drivewealth_user_id}):
        provider.sync_user(account.drivewealth_user_id)

    trading_repository.persist(account)
    account: DriveWealthAccount = trading_repository.refresh(account)
    if not account.trading_account_id:
        trading_account = TradingAccount()
        trading_account.profile_id = profile_id
        trading_account.name = account.nickname
        account.update_trading_account(trading_account)
        trading_repository.persist(trading_account)
        account.trading_account_id = trading_account.id
    trading_repository.persist(account)

    portfolio = DriveWealthPortfolio()
    portfolio.profile_id = profile_id
    portfolio.drivewealth_account_id = account_ref_id
    portfolio.ref_id = portfolio_ref_id
    data = api.get_portfolio(portfolio)
    portfolio.set_from_response(data)
    trading_repository.persist(portfolio)

    collection_version = TradingCollectionVersion()
    collection_version.target_amount_delta = target_amount_delta

    fund = DriveWealthFund()
    fund.ref_id = fund_ref_id

    helper = DriveWealthProviderRebalanceHelper(provider, trading_repository)
    helper.handle_cash_amount_change(collection_version, portfolio, fund,
                                     False)

    portfolio.normalize_weights()
    provider.send_portfolio_to_api(portfolio)
    provider.api.create_autopilot_run([portfolio.drivewealth_account_id])
