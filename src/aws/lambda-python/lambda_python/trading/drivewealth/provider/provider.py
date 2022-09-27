import os
from decimal import Decimal
from typing import List

from gainy.exceptions import NotFoundException
from portfolio.plaid import PlaidService
from portfolio.plaid.models import PlaidAccessToken
from trading.models import TradingMoneyFlow, FundingAccount, TradingCollectionVersion
from trading.drivewealth.provider.collection import DriveWealthProviderCollection
from trading.drivewealth.provider.kyc import DriveWealthProviderKYC
from trading.drivewealth.models import DriveWealthBankAccount, DriveWealthDeposit, \
    DriveWealthRedemption, DriveWealthAutopilotRun, DriveWealthPortfolio
from trading.drivewealth.api import DriveWealthApi
from trading.drivewealth.repository import DriveWealthRepository

from gainy.utils import get_logger
from gainy.trading.models import TradingAccount
from gainy.trading.drivewealth.models import DriveWealthAccount, DriveWealthUser
from gainy.trading.drivewealth import DriveWealthProvider as GainyDriveWealthProvider

logger = get_logger(__name__)

IS_UAT = os.getenv("DRIVEWEALTH_IS_UAT", "true") != "false"


class DriveWealthProvider(GainyDriveWealthProvider, DriveWealthProviderKYC,
                          DriveWealthProviderCollection):

    def __init__(self, repository: DriveWealthRepository, api: DriveWealthApi,
                 plaid_service: PlaidService):
        self.repository = repository
        self.plaid_service = plaid_service
        self.api = api

    def link_bank_account_with_plaid(
            self, access_token: PlaidAccessToken, account_id: str,
            account_name: str) -> DriveWealthBankAccount:
        repository = self.repository
        bank_account = repository.find_one(
            DriveWealthBankAccount, {"plaid_access_token_id": access_token.id})
        if bank_account:
            return bank_account

        processor_token = self.plaid_service.create_processor_token(
            access_token.access_token, account_id, "drivewealth")

        user = self._get_user(access_token.profile_id)
        data = self.api.link_bank_account(user.ref_id, processor_token,
                                          account_name)

        entity = DriveWealthBankAccount()
        entity.set_from_response(data)
        entity.plaid_access_token_id = access_token.id
        entity.plaid_account_id = account_id
        repository.persist(entity)

        return entity

    def delete_funding_account(self, funding_account_id: int):
        drivewealth_bank_account = self.repository.find_one(
            DriveWealthBankAccount, {"funding_account_id": funding_account_id})
        self.api.delete_bank_account(drivewealth_bank_account.ref_id)
        self.repository.delete(drivewealth_bank_account)

    def transfer_money(self, money_flow: TradingMoneyFlow, amount: Decimal,
                       trading_account_id: int, funding_account_id: int):
        trading_account = self.repository.find_one(
            DriveWealthAccount, {"trading_account_id": trading_account_id})
        bank_account = self.repository.find_one(
            DriveWealthBankAccount, {"funding_account_id": funding_account_id})

        if amount > 0:
            response = self.api.create_deposit(amount, trading_account,
                                               bank_account)
            entity = DriveWealthDeposit()
        else:
            response = self.api.create_redemption(amount, trading_account,
                                                  bank_account)
            entity = DriveWealthRedemption()

        entity.set_from_response(response)
        entity.trading_account_ref_id = trading_account.ref_id
        entity.bank_account_ref_id = bank_account.ref_id
        entity.money_flow_id = money_flow.id
        self._update_money_flow_status(entity, money_flow)
        self.repository.persist(entity)

        self._on_money_transfer(money_flow.profile_id)

        return entity

    def debug_add_money(self, trading_account_id, amount):
        if not IS_UAT:
            raise Exception('Not supported in production')

        account: DriveWealthAccount = self.repository.find_one(
            DriveWealthAccount, {"trading_account_id": trading_account_id})

        if not account:
            raise NotFoundException()

        self.api.add_money(account.ref_id, amount)

    def debug_delete_data(self, profile_id):
        if not IS_UAT:
            raise Exception('Not supported in production')

        repository = self.repository
        user = self._get_user(profile_id)
        repository.delete(user)

        for entity in repository.find_all(TradingAccount,
                                          {"profile_id": profile_id}):
            repository.delete(entity)

        for entity in repository.find_all(FundingAccount,
                                          {"profile_id": profile_id}):
            repository.delete(entity)

        for entity in repository.find_all(TradingCollectionVersion,
                                          {"profile_id": profile_id}):
            repository.delete(entity)

        for entity in repository.find_all(TradingMoneyFlow,
                                          {"profile_id": profile_id}):
            repository.delete(entity)

    def sync_data(self, profile_id):
        user = self._get_user(profile_id)

        self.sync_user(user.ref_id)
        self.sync_profile_trading_accounts(profile_id)
        self._sync_autopilot_runs(user.ref_id)
        self._sync_bank_accounts(user.ref_id)
        self._sync_user_deposits(user.ref_id)
        # self._sync_documents(user.ref_id)
        self._sync_portfolio_statuses(profile_id)
        # self._sync_redemptions(user.ref_id)
        # self._sync_users(user.ref_id)

    def sync_deposit(self, deposit_ref_id: str, fetch_info=False):
        repository = self.repository

        entity = repository.find_one(DriveWealthDeposit,
                                     {"ref_id": deposit_ref_id})
        if not entity:
            entity = DriveWealthDeposit()
            fetch_info = True

        if fetch_info:
            deposit_data = self.api.get_deposit(deposit_ref_id)
            entity.set_from_response(deposit_data)
            repository.persist(entity)

        if not entity.money_flow_id:
            return
        money_flow = repository.find_one(TradingMoneyFlow,
                                         {"id": entity.money_flow_id})
        self._update_money_flow_status(entity, money_flow)
        repository.persist(money_flow)

    def _sync_bank_accounts(self, user_ref_id):
        repository = self.repository

        bank_accounts_data = self.api.get_user_bank_accounts(user_ref_id)
        for bank_account_data in bank_accounts_data:
            entity = repository.find_one(DriveWealthBankAccount,
                                         {"ref_id": bank_account_data['id']
                                          }) or DriveWealthBankAccount()
            entity.set_from_response(bank_account_data)
            entity.drivewealth_user_id = user_ref_id
            return repository.persist(entity)

    def _sync_user_deposits(self, user_ref_id: str):
        repository = self.repository

        deposits_data = self.api.get_user_deposits(user_ref_id)
        for deposit_data in deposits_data:
            entity = repository.find_one(
                DriveWealthDeposit,
                {"ref_id": deposit_data['id']}) or DriveWealthDeposit()
            entity.set_from_response(deposit_data)
            repository.persist(entity)

            self.sync_deposit(deposit_ref_id=entity.ref_id, fetch_info=False)

    def _sync_autopilot_runs(self, user_ref_id):
        repository = self.repository

        accounts: List[DriveWealthAccount] = repository.find_all(
            DriveWealthAccount, {"drivewealth_user_id": user_ref_id})
        for account in accounts:
            autopilot_runs: List[
                DriveWealthAutopilotRun] = repository.find_all(
                    DriveWealthAutopilotRun, {"account_id": account.ref_id})

            for entity in autopilot_runs:
                data = self.api.get_autopilot_run(entity.ref_id)
                entity.set_from_response(data)
                repository.persist(entity)

                if not entity.collection_version_id:
                    continue
                trading_collection_version = repository.find_one(
                    TradingCollectionVersion,
                    {"id": entity.collection_version_id})
                entity._update_trading_collection_version(
                    trading_collection_version)
                repository.persist(trading_collection_version)

    def _sync_portfolio_statuses(self, profile_id):
        repository = self.repository

        portfolios: List[DriveWealthPortfolio] = repository.find_all(
            DriveWealthPortfolio, {"profile_id": profile_id})
        for portfolio in portfolios:
            self._get_portfolio_status(portfolio)

    def _update_money_flow_status(self, entity, money_flow: TradingMoneyFlow):
        money_flow.status = entity.status
