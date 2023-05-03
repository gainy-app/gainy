from _decimal import Decimal

from gainy.context_container import ContextContainer
from gainy.trading.drivewealth.models import DriveWealthPortfolio, DriveWealthFund, DriveWealthAccount, DriveWealthUser
from gainy.trading.drivewealth.provider.rebalance_helper import DriveWealthProviderRebalanceHelper
from gainy.trading.exceptions import InsufficientFundsException
from gainy.trading.models import TradingCollectionVersion, TradingAccount
from gainy.utils import get_logger

logger = get_logger(__name__)
'''
select *
from app.trading_orders
where status = 'EXECUTED_FULLY'
  and (executed_amount - target_amount_delta) > 1;

with order_stats as
         (
             select profile_id,
                    'ttf_' || profile_id || '_' || collection_id as holding_group_id,
                    sum(executed_amount)                         as executed_amount,
                    collection_id,
                    null::varchar                                as symbol
             from app.trading_collection_versions
                      left join (
                                    select profile_id, collection_id, max(id) as last_selloff_id
                                    from app.trading_collection_versions
                                    where target_amount_delta_relative = -1
                                    group by profile_id, collection_id
                                ) t using (profile_id, collection_id)
             where id > last_selloff_id
                or last_selloff_id is null
             group by profile_id, collection_id

             union all

             select profile_id,
                    'ticker_' || profile_id || '_' || symbol as holding_group_id,
                    sum(executed_amount)                     as executed_amount,
                    null::int                                as collection_id,
                    symbol
             from app.trading_orders
                      left join (
                                    select profile_id, symbol, max(id) as last_selloff_id
                                    from app.trading_orders
                                    where target_amount_delta_relative = -1
                                    group by profile_id, symbol
                                ) t using (profile_id, symbol)
             where id > last_selloff_id
                or last_selloff_id is null
             group by profile_id, symbol
         )
select order_stats.profile_id,
       drivewealth_funds.collection_id,
       drivewealth_account_id                                                         as account_ref_id,
       drivewealth_portfolios.ref_id                                                  as portfolio_ref_id,
       drivewealth_funds.ref_id                                                       as fund_ref_id,
       executed_amount + coalesce(absolute_gain_total, 0) - coalesce(actual_value, 0) as target_amount_delta,
       executed_amount,
       absolute_gain_total,
       actual_value,
       drivewealth_portfolio_holding_group_gains.updated_at,
       last_rebalance_at
from order_stats
         left join drivewealth_portfolio_holding_group_gains using (profile_id, holding_group_id)
         join app.drivewealth_portfolios using (profile_id)
         left join app.drivewealth_funds
                   on drivewealth_funds.profile_id = order_stats.profile_id
                       and (drivewealth_funds.collection_id = order_stats.collection_id
                           or drivewealth_funds.symbol = order_stats.symbol)
where abs(executed_amount + coalesce(absolute_gain_total, 0) - coalesce(actual_value, 0)) > 10
order by profile_id, order_stats.collection_id, order_stats.symbol, executed_amount + coalesce(absolute_gain_total, 0) - coalesce(actual_value, 0)
'''

data = []

accounts_to_rebalance = set()

with ContextContainer() as context_container:
    for row in data:
        print()
        print("=" * 30)
        print(row)
        print("=" * 30)
        print()

        profile_id = row["profile_id"]
        account_ref_id = row["account_ref_id"]
        portfolio_ref_id = row["portfolio_ref_id"]
        fund_ref_id = row["fund_ref_id"]
        target_amount_delta = Decimal(row["target_amount_delta"])

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
        portfolio_data = api.get_portfolio(portfolio)
        portfolio.set_from_response(portfolio_data)
        trading_repository.persist(portfolio)

        portfolio_status = provider.sync_portfolio_status(portfolio, force=True)
        fund_holdings = portfolio_status.get_fund(fund_ref_id)

        if not fund_holdings:
            # TODO really continue?
            continue

        fund_weight_delta = target_amount_delta / portfolio_status.equity_value
        new_target_weight = fund_holdings.actual_weight + fund_weight_delta
        new_target_amount_delta = (
            new_target_weight -
            fund_holdings.target_weight) * portfolio_status.equity_value
        print('>>>>>>> ', new_target_weight, new_target_amount_delta, row,
              fund_holdings.data)

        collection_version = TradingCollectionVersion()
        collection_version.target_amount_delta = new_target_amount_delta

        fund = DriveWealthFund()
        fund.ref_id = fund_ref_id

        helper = DriveWealthProviderRebalanceHelper(provider,
                                                    trading_repository)
        try:
            helper.handle_cash_amount_change(collection_version, portfolio,
                                             fund, False)
        except InsufficientFundsException as e:
            logger.info(e)
            continue

        provider.send_portfolio_to_api(portfolio)
        accounts_to_rebalance.add(row["account_ref_id"])

    provider.api.create_autopilot_run(list(accounts_to_rebalance))
