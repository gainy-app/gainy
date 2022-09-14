from decimal import Decimal

from portfolio.plaid import PlaidService
from portfolio.plaid.models import PlaidAccessToken
from trading.models import TradingMoneyFlow
from trading.drivewealth.provider.collection import DriveWealthProviderCollection
from trading.drivewealth.provider.kyc import DriveWealthProviderKYC
from trading.drivewealth.models import DriveWealthBankAccount, DriveWealthAccount, DriveWealthDeposit, DriveWealthRedemption
from trading.drivewealth.api import DriveWealthApi
from trading.drivewealth.repository import DriveWealthRepository
from gainy.utils import get_logger

logger = get_logger(__name__)


class DriveWealthProvider(DriveWealthProviderKYC,
                          DriveWealthProviderCollection):

    def __init__(self, drivewealth_repository: DriveWealthRepository,
                 api: DriveWealthApi, plaid_service: PlaidService):
        self.drivewealth_repository = drivewealth_repository
        self.plaid_service = plaid_service
        self.api = api

    def link_bank_account_with_plaid(
            self, access_token: PlaidAccessToken, account_id: str,
            account_name: str) -> DriveWealthBankAccount:
        repository = self.drivewealth_repository
        bank_account = repository.find_one(
            DriveWealthBankAccount, {"plaid_access_token_id": access_token.id})
        if bank_account:
            return bank_account

        processor_token = self.plaid_service.create_processor_token(
            access_token.access_token, account_id, "drivewealth")

        user = repository.get_user(access_token.profile_id)
        if user is None:
            raise Exception("KYC form has not been sent")

        data = self.api.link_bank_account(user.ref_id, processor_token,
                                          account_name)

        entity = DriveWealthBankAccount()
        entity.set_from_response(data)
        entity.plaid_access_token_id = access_token.id
        entity.plaid_account_id = account_id

        return repository.persist(entity)

    def delete_funding_account(self, funding_account_id: int):
        drivewealth_bank_account = self.drivewealth_repository.find_one(
            DriveWealthBankAccount, {"funding_account_id": funding_account_id})
        self.api.delete_bank_account(drivewealth_bank_account.ref_id)
        self.drivewealth_repository.delete(drivewealth_bank_account)

    def transfer_money(self, money_flow: TradingMoneyFlow, amount: Decimal,
                       trading_account_id: int, funding_account_id: int):
        trading_account = self.drivewealth_repository.find_one(
            DriveWealthAccount, {"trading_account_id": trading_account_id})
        bank_account = self.drivewealth_repository.find_one(
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
        self.drivewealth_repository.persist(entity)

        self._on_money_transfer(money_flow.profile_id)

        return entity

    def sync_data(self, profile_id):
        repository = self.drivewealth_repository
        user = repository.get_user(profile_id)
        if user is None:
            return

        self._sync_trading_accounts(user)
        # self._sync_autopilot_runs(user.ref_id)
        self._sync_bank_accounts(user.ref_id)
        self._sync_deposits(user.ref_id)
        # self._sync_documents(user.ref_id)
        # self._sync_portfolio_statuses(user.ref_id)
        # self._sync_redemptions(user.ref_id)
        # self._sync_users(user.ref_id)

    def _sync_bank_accounts(self, user_ref_id):
        repository = self.drivewealth_repository

        bank_accounts_data = self.api.get_user_bank_accounts(user_ref_id)
        for bank_account_data in bank_accounts_data:
            entity = repository.find_one(DriveWealthBankAccount, {"ref_id": bank_account_data['id']}) or DriveWealthBankAccount()
            entity.set_from_response(bank_account_data)
            entity.drivewealth_user_id = user_ref_id
            return repository.persist(entity)

    def _sync_deposits(self, user_ref_id):
        repository = self.drivewealth_repository

        deposits_data = self.api.get_user_deposits(user_ref_id)
        for deposit_data in deposits_data:
            entity = repository.find_one(DriveWealthDeposit, {"ref_id": deposit_data['id']}) or DriveWealthDeposit()
            entity.set_from_response(deposit_data)
            repository.persist(entity)

            if not entity.money_flow_id:
                continue
            money_flow = repository.find_one(TradingMoneyFlow, {"id": entity.money_flow_id})
            self._update_money_flow_status(entity, money_flow)
            repository.persist(money_flow)

    def _update_money_flow_status(self, entity, money_flow: TradingMoneyFlow):
        money_flow.status = entity.status
