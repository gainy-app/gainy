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

data = [{
    "profile_id": 6,
    "collection_id": 90,
    "account_ref_id": "f86b2342-d9cf-4779-a203-d4f37817101e.1671298320252",
    "portfolio_ref_id": "portfolio_baf09090-0097-49ca-8939-f056af0c20ab",
    "fund_ref_id": "fund_60c29d69-9c28-46c5-8a77-a5db6fe9841b",
    "target_amount_delta": -11.520000000000039,
    "last_rebalance_at": "2023-04-14 19:45:04.000000 +00:00"
}, {
    "profile_id": 6,
    "collection_id": 116,
    "account_ref_id": "f86b2342-d9cf-4779-a203-d4f37817101e.1671298320252",
    "portfolio_ref_id": "portfolio_baf09090-0097-49ca-8939-f056af0c20ab",
    "fund_ref_id": "fund_340f385b-ebb9-4663-9ae8-33853480b510",
    "target_amount_delta": -12.76170212765902,
    "last_rebalance_at": "2023-04-14 19:45:04.000000 +00:00"
}, {
    "profile_id": 6,
    "collection_id": 154,
    "account_ref_id": "f86b2342-d9cf-4779-a203-d4f37817101e.1671298320252",
    "portfolio_ref_id": "portfolio_baf09090-0097-49ca-8939-f056af0c20ab",
    "fund_ref_id": "fund_025c61d0-139e-4791-bd24-f57c80f6828d",
    "target_amount_delta": -19.450000000001012,
    "last_rebalance_at": "2023-04-14 19:45:04.000000 +00:00"
}, {
    "profile_id": 6,
    "collection_id": 252,
    "account_ref_id": "f86b2342-d9cf-4779-a203-d4f37817101e.1671298320252",
    "portfolio_ref_id": "portfolio_baf09090-0097-49ca-8939-f056af0c20ab",
    "fund_ref_id": "fund_2817e505-0be8-4214-a1d1-6f419c7bff64",
    "target_amount_delta": 118.15000000000009,
    "last_rebalance_at": "2023-04-14 19:45:04.000000 +00:00"
}, {
    "profile_id": 67,
    "collection_id": 65,
    "account_ref_id": "1aa249c0-0186-48ae-9173-a025780634f1.1671224113114",
    "portfolio_ref_id": "portfolio_9def6a6e-a026-4848-a6a9-dad851100c98",
    "fund_ref_id": "fund_418cecf7-c069-4a8a-bdb8-1fe810c93de1",
    "target_amount_delta": 11.06000000000006,
    "last_rebalance_at": "2023-04-14 14:19:17.000000 +00:00"
}, {
    "profile_id": 67,
    "collection_id": 74,
    "account_ref_id": "1aa249c0-0186-48ae-9173-a025780634f1.1671224113114",
    "portfolio_ref_id": "portfolio_9def6a6e-a026-4848-a6a9-dad851100c98",
    "fund_ref_id": "fund_77106a78-bef0-499a-821f-26f25006af99",
    "target_amount_delta": 14.230000000000018,
    "last_rebalance_at": "2023-04-14 14:19:17.000000 +00:00"
}, {
    "profile_id": 67,
    "collection_id": 145,
    "account_ref_id": "1aa249c0-0186-48ae-9173-a025780634f1.1671224113114",
    "portfolio_ref_id": "portfolio_9def6a6e-a026-4848-a6a9-dad851100c98",
    "fund_ref_id": "fund_b3f4304d-8554-424f-999a-66f38d18528a",
    "target_amount_delta": -15.6099999999999,
    "last_rebalance_at": "2023-04-14 14:19:17.000000 +00:00"
}, {
    "profile_id": 67,
    "collection_id": 205,
    "account_ref_id": "1aa249c0-0186-48ae-9173-a025780634f1.1671224113114",
    "portfolio_ref_id": "portfolio_9def6a6e-a026-4848-a6a9-dad851100c98",
    "fund_ref_id": "fund_5090227f-a14e-48d7-910c-0c02b267fe99",
    "target_amount_delta": 12.473896178766609,
    "last_rebalance_at": "2023-04-14 14:19:17.000000 +00:00"
}, {
    "profile_id": 67,
    "collection_id": 253,
    "account_ref_id": "1aa249c0-0186-48ae-9173-a025780634f1.1671224113114",
    "portfolio_ref_id": "portfolio_9def6a6e-a026-4848-a6a9-dad851100c98",
    "fund_ref_id": "fund_c1ee6a43-40be-48d4-899f-4871ad12e99b",
    "target_amount_delta": -20.18621292556179,
    "last_rebalance_at": "2023-04-14 14:19:17.000000 +00:00"
}, {
    "profile_id": 108,
    "collection_id": 83,
    "account_ref_id": "8e9d7115-e089-4f40-a00f-4cdeeb122273.1676683075008",
    "portfolio_ref_id": "portfolio_26976962-edf7-4bc5-8ec0-c6f414c97b83",
    "fund_ref_id": "fund_04c3ebee-0520-423e-bd58-d9e00c1d15ef",
    "target_amount_delta": 10.259999999999977,
    "last_rebalance_at": "2023-04-14 14:19:17.000000 +00:00"
}, {
    "profile_id": 168,
    "collection_id": 65,
    "account_ref_id": "c669f68f-06b0-4573-b2a5-dad346ac1b75.1675794457892",
    "portfolio_ref_id": "portfolio_c09d76c2-b020-4b2f-8ccf-0b95bf4f3704",
    "fund_ref_id": "fund_2310a99b-c015-4eac-9acf-382e392d7c1f",
    "target_amount_delta": -109.26999999999862,
    "last_rebalance_at": "2023-04-14 14:19:17.000000 +00:00"
}, {
    "profile_id": 168,
    "collection_id": 125,
    "account_ref_id": "c669f68f-06b0-4573-b2a5-dad346ac1b75.1675794457892",
    "portfolio_ref_id": "portfolio_c09d76c2-b020-4b2f-8ccf-0b95bf4f3704",
    "fund_ref_id": "fund_99354528-bfc4-456a-8316-6c466a442f9c",
    "target_amount_delta": 39.270000000000095,
    "last_rebalance_at": "2023-04-14 14:19:17.000000 +00:00"
}, {
    "profile_id": 168,
    "collection_id": 205,
    "account_ref_id": "c669f68f-06b0-4573-b2a5-dad346ac1b75.1675794457892",
    "portfolio_ref_id": "portfolio_c09d76c2-b020-4b2f-8ccf-0b95bf4f3704",
    "fund_ref_id": "fund_83f5c785-b60e-4241-9df2-b1406ae4d01b",
    "target_amount_delta": 35.34432316180187,
    "last_rebalance_at": "2023-04-14 14:19:17.000000 +00:00"
}, {
    "profile_id": 168,
    "collection_id": 241,
    "account_ref_id": "c669f68f-06b0-4573-b2a5-dad346ac1b75.1675794457892",
    "portfolio_ref_id": "portfolio_c09d76c2-b020-4b2f-8ccf-0b95bf4f3704",
    "fund_ref_id": "fund_6601a8e9-a577-4561-a3ca-d0d40bd68b76",
    "target_amount_delta": -14.40476413114095,
    "last_rebalance_at": "2023-04-14 14:19:17.000000 +00:00"
}, {
    "profile_id": 22945,
    "collection_id": 252,
    "account_ref_id": "2a1137b1-ad93-4785-984a-4d3748d888ba.1672764303972",
    "portfolio_ref_id": "portfolio_729f9f78-4f20-46b0-bfa9-dd543a83ca9d",
    "fund_ref_id": "fund_8c799d16-f063-42d3-821b-435a6a36c297",
    "target_amount_delta": 12.161539633445518,
    "last_rebalance_at": "2023-04-14 14:19:17.000000 +00:00"
}, {
    "profile_id": 23347,
    "collection_id": 125,
    "account_ref_id": "f6c1e732-1760-4689-bee3-c4ef750480fc.1675989003529",
    "portfolio_ref_id": "portfolio_e2419564-e325-4cef-ae9a-768f3522f1c5",
    "fund_ref_id": "fund_7bbfecc1-995b-4817-a5d0-6fd0459103d3",
    "target_amount_delta": 19.69999999999999,
    "last_rebalance_at": "2023-04-14 14:19:17.000000 +00:00"
}, {
    "profile_id": 23721,
    "collection_id": 92,
    "account_ref_id": "27b26762-0210-4831-91e6-c7594567c5ec.1677971860346",
    "portfolio_ref_id": "portfolio_103db0be-b17b-4326-8b94-0bb9764bed95",
    "fund_ref_id": "fund_a8ea9165-09f9-46c7-a6d2-31422b000ab6",
    "target_amount_delta": 12.739999999999952,
    "last_rebalance_at": "2023-04-14 14:19:17.000000 +00:00"
}, {
    "profile_id": 23725,
    "collection_id": 247,
    "account_ref_id": "352fda76-897a-4588-b806-a445a62cc0dd.1677971897654",
    "portfolio_ref_id": "portfolio_751fbaa1-6c23-4cfe-82e9-516136cea854",
    "fund_ref_id": "fund_63812d05-8571-4f0e-b370-bedba52e5d34",
    "target_amount_delta": 40.099999999999994,
    "last_rebalance_at": "2023-04-14 14:19:17.000000 +00:00"
}]

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
        data = api.get_portfolio(portfolio)
        portfolio.set_from_response(data)
        trading_repository.persist(portfolio)

        collection_version = TradingCollectionVersion()
        collection_version.target_amount_delta = target_amount_delta

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

        portfolio.normalize_weights()
        provider.send_portfolio_to_api(portfolio)
        provider.api.create_autopilot_run([portfolio.drivewealth_account_id])
