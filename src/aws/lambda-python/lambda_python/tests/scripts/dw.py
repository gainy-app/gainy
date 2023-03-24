from _decimal import Decimal

from gainy.context_container import ContextContainer
from gainy.trading.drivewealth.models import DriveWealthPortfolio, DriveWealthFund, DriveWealthAccount, DriveWealthUser
from gainy.trading.drivewealth.provider.rebalance_helper import DriveWealthProviderRebalanceHelper
from gainy.trading.models import TradingCollectionVersion, TradingAccount

# profile_id = 6
# account_ref_id = 'f86b2342-d9cf-4779-a203-d4f37817101e.1671298320252'
# portfolio_ref_id = 'portfolio_baf09090-0097-49ca-8939-f056af0c20ab'
# fund_ref_id = 'fund_2817e505-0be8-4214-a1d1-6f419c7bff64'
# target_amount_delta = Decimal(1.2000000000000455)

# profile_id = 6
# account_ref_id = 'f86b2342-d9cf-4779-a203-d4f37817101e.1671298320252'
# portfolio_ref_id = 'portfolio_baf09090-0097-49ca-8939-f056af0c20ab'
# fund_ref_id = 'fund_025c61d0-139e-4791-bd24-f57c80f6828d'
# target_amount_delta = Decimal(-1.809999999999718)

# profile_id = 67
# account_ref_id = '1aa249c0-0186-48ae-9173-a025780634f1.1671224113114'
# portfolio_ref_id = 'portfolio_9def6a6e-a026-4848-a6a9-dad851100c98'
# fund_ref_id = 'fund_95a2f478-a260-4d06-b1fd-99ab7b70b4ea'
# target_amount_delta = Decimal(2.585107663418512)

# profile_id = 67
# account_ref_id = '1aa249c0-0186-48ae-9173-a025780634f1.1671224113114'
# portfolio_ref_id = 'portfolio_9def6a6e-a026-4848-a6a9-dad851100c98'
# fund_ref_id = 'fund_b3f4304d-8554-424f-999a-66f38d18528a'
# target_amount_delta = Decimal(-2.7799999999999727)

# profile_id = 67
# account_ref_id = '1aa249c0-0186-48ae-9173-a025780634f1.1671224113114'
# portfolio_ref_id = 'portfolio_9def6a6e-a026-4848-a6a9-dad851100c98'
# fund_ref_id = 'fund_418cecf7-c069-4a8a-bdb8-1fe810c93de1'
# target_amount_delta = Decimal(5.0499999999999545)

# profile_id = 168
# account_ref_id = 'c669f68f-06b0-4573-b2a5-dad346ac1b75.1675794457892'
# portfolio_ref_id = 'portfolio_c09d76c2-b020-4b2f-8ccf-0b95bf4f3704'
# fund_ref_id = 'fund_4b13c863-a837-4a67-8ee1-d794924a70d8'
# target_amount_delta = Decimal(-7.909999999999968)

# profile_id = 168
# account_ref_id = 'c669f68f-06b0-4573-b2a5-dad346ac1b75.1675794457892'
# portfolio_ref_id = 'portfolio_c09d76c2-b020-4b2f-8ccf-0b95bf4f3704'
# fund_ref_id = 'fund_ff637c70-8c1b-4467-a082-2c16e2355929'
# target_amount_delta = Decimal(3.160000000000082)

# profile_id = 168
# account_ref_id = 'c669f68f-06b0-4573-b2a5-dad346ac1b75.1675794457892'
# portfolio_ref_id = 'portfolio_c09d76c2-b020-4b2f-8ccf-0b95bf4f3704'
# fund_ref_id = 'fund_6601a8e9-a577-4561-a3ca-d0d40bd68b76'
# target_amount_delta = Decimal(-1.046222051786458)

# profile_id = 168
# account_ref_id = 'c669f68f-06b0-4573-b2a5-dad346ac1b75.1675794457892'
# portfolio_ref_id = 'portfolio_c09d76c2-b020-4b2f-8ccf-0b95bf4f3704'
# fund_ref_id = 'fund_2310a99b-c015-4eac-9acf-382e392d7c1f'
# target_amount_delta = Decimal(10.330000000000837)

# profile_id = 168
# account_ref_id = 'c669f68f-06b0-4573-b2a5-dad346ac1b75.1675794457892'
# portfolio_ref_id = 'portfolio_c09d76c2-b020-4b2f-8ccf-0b95bf4f3704'
# fund_ref_id = 'fund_99354528-bfc4-456a-8316-6c466a442f9c'
# target_amount_delta = Decimal(28.85000000000025)

# profile_id = 21190
# account_ref_id = '8b15d1f9-5ccf-46af-8a59-25e99a188948.1673479067157'
# portfolio_ref_id = 'portfolio_a1cae003-a2ef-4162-8683-5a3f4f8a9ab8'
# fund_ref_id = 'fund_ddf49caf-2f6f-47c3-86c8-4f67179243c4'
# target_amount_delta = Decimal(-2.6604740369146214)

# profile_id = 21190
# account_ref_id = '8b15d1f9-5ccf-46af-8a59-25e99a188948.1673479067157'
# portfolio_ref_id = 'portfolio_a1cae003-a2ef-4162-8683-5a3f4f8a9ab8'
# fund_ref_id = 'fund_4c5b0610-5a55-455a-b2e4-51419db77af2'
# target_amount_delta = Decimal(2.999999999999993)

# profile_id = 21190
# account_ref_id = '8b15d1f9-5ccf-46af-8a59-25e99a188948.1673479067157'
# portfolio_ref_id = 'portfolio_a1cae003-a2ef-4162-8683-5a3f4f8a9ab8'
# fund_ref_id = 'fund_c910df64-a14b-4d36-9bef-f7f569418833'
# target_amount_delta = Decimal(3.3400000000000016)

# profile_id = 22945
# account_ref_id = '2a1137b1-ad93-4785-984a-4d3748d888ba.1672764303972'
# portfolio_ref_id = 'portfolio_729f9f78-4f20-46b0-bfa9-dd543a83ca9d'
# fund_ref_id = ''
# target_amount_delta = Decimal()

# profile_id = 23725
# account_ref_id = '352fda76-897a-4588-b806-a445a62cc0dd.1677971897654'
# portfolio_ref_id = 'portfolio_751fbaa1-6c23-4cfe-82e9-516136cea854'
# fund_ref_id = ''
# target_amount_delta = Decimal()

# profile_id = 23347
# account_ref_id = 'f6c1e732-1760-4689-bee3-c4ef750480fc.1675989003529'
# portfolio_ref_id = 'portfolio_e2419564-e325-4cef-ae9a-768f3522f1c5'
# fund_ref_id = 'fund_971b4a90-18f5-4d47-a55f-2aba8a5250e1'
# target_amount_delta = Decimal(11.929998168945247)

# profile_id = 23347 # MANUAL
# account_ref_id = 'f6c1e732-1760-4689-bee3-c4ef750480fc.1675989003529'
# portfolio_ref_id = 'portfolio_e2419564-e325-4cef-ae9a-768f3522f1c5'
# fund_ref_id = 'fund_7bbfecc1-995b-4817-a5d0-6fd0459103d3'
# target_amount_delta = Decimal(12.810000000000002)

# profile_id = 23414
# account_ref_id = '2320b9ea-12d6-4b48-89f6-d0a40d7dad86.1676576039209'
# portfolio_ref_id = 'portfolio_0c343d62-9c6a-4007-ae11-7d73ef1de964'
# fund_ref_id = 'fund_9155468c-d7f9-4961-962d-b4df337b1c25'
# target_amount_delta = Decimal(-1.8299999999999699)

# profile_id = 23721 MANUAL

# profile_id = 23731
# account_ref_id = 'ed09ad71-3897-4a83-83bb-4de61afb822c.1677971984350'
# portfolio_ref_id = 'portfolio_deda016b-c33a-4502-8914-e587d15bc923'
# fund_ref_id = 'fund_c09626e9-9d6f-4762-82a4-282c245b55e9'
# target_amount_delta = Decimal(1.4300000000000068)


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
    account:DriveWealthAccount = trading_repository.refresh(account)
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
    helper.handle_cash_amount_change(collection_version, portfolio, fund, False)

    portfolio.normalize_weights()
    provider.send_portfolio_to_api(portfolio)
    provider.api.create_autopilot_run([portfolio.drivewealth_account_id])
