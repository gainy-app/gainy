import os
from decimal import Decimal

from gainy.data_access.repository import MAX_TRANSACTION_SIZE
from gainy.exceptions import NotFoundException
from gainy.trading.drivewealth.exceptions import DriveWealthApiException
from portfolio.plaid import PlaidService
from gainy.plaid.models import PlaidAccessToken
from trading.models import TradingMoneyFlow, TradingStatement
from trading.drivewealth.provider.collection import DriveWealthProviderCollection
from trading.drivewealth.provider.kyc import DriveWealthProviderKYC
from trading.drivewealth.models import DriveWealthBankAccount, DriveWealthDeposit, \
    DriveWealthRedemption, DriveWealthAutopilotRun, BaseDriveWealthMoneyFlowModel, DriveWealthStatement
from trading.drivewealth.api import DriveWealthApi
from trading.drivewealth.repository import DriveWealthRepository

from gainy.utils import get_logger
from gainy.trading.models import FundingAccount, TradingAccount, TradingCollectionVersion, TradingMoneyFlowStatus
from gainy.trading.drivewealth.models import DriveWealthAccount, DriveWealthUser

logger = get_logger(__name__)

IS_UAT = os.getenv("DRIVEWEALTH_IS_UAT", "true") != "false"


class DriveWealthProvider(DriveWealthProviderKYC,
                          DriveWealthProviderCollection):

    def __init__(self, repository: DriveWealthRepository, api: DriveWealthApi,
                 plaid_service: PlaidService):
        super().__init__(repository, api)
        self.plaid_service = plaid_service

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

        user: DriveWealthUser = self.repository.find_one(
            DriveWealthUser, {"ref_id": account.drivewealth_user_id})
        money_flow = TradingMoneyFlow()
        money_flow.profile_id = user.profile_id
        money_flow.amount = amount
        money_flow.status = TradingMoneyFlowStatus.SUCCESS
        money_flow.trading_account_id = trading_account_id
        self.repository.persist(money_flow)

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
        self._sync_autopilot_runs()
        self._sync_bank_accounts(user.ref_id)
        self._sync_user_deposits(user.ref_id)
        # self._sync_statements(profile_id)
        self.sync_portfolios(profile_id)

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
            if not repository.find_one(
                    DriveWealthAccount,
                {"ref_id": entity.trading_account_ref_id}):
                self.sync_trading_account(
                    account_ref_id=entity.trading_account_ref_id,
                    fetch_info=True)
            repository.persist(entity)

        if not entity.money_flow_id:
            return
        money_flow = repository.find_one(TradingMoneyFlow,
                                         {"id": entity.money_flow_id})
        self._update_money_flow_status(entity, money_flow)
        repository.persist(money_flow)

    def sync_autopilot_run(self, entity: DriveWealthAutopilotRun):
        repository = self.repository
        try:
            data = self.api.get_autopilot_run(entity.ref_id)
        except DriveWealthApiException as e:
            logger.warning(e)
            return

        entity.set_from_response(data)
        repository.persist(entity)

    def handle_redemption_status(self, redemption: DriveWealthRedemption):
        if redemption.status == 'RIA_Pending':
            self.api.update_redemption(redemption, status='RIA_Approved')

    def download_statement(self, statement: TradingStatement) -> str:
        dw_statement = self.repository.find_one(
            DriveWealthStatement, {"trading_statement_id": statement.id})
        if not dw_statement:
            raise NotFoundException

        return self.api.get_statement_url(dw_statement)

    def create_trading_statements(self, entities: list[DriveWealthStatement],
                                  profile_id):
        for dw_statement in entities:
            if dw_statement.trading_statement_id:
                continue

            trading_statement = TradingStatement()
            trading_statement.profile_id = profile_id
            trading_statement.type = dw_statement.type
            trading_statement.display_name = dw_statement.display_name
            self.repository.persist(trading_statement)
            dw_statement.trading_statement_id = trading_statement.id
            self.repository.persist(dw_statement)

    def get_profile_id_by_user_id(self, user_ref_id: str) -> int:
        user: DriveWealthUser = self.repository.find_one(
            DriveWealthUser, {"ref_id": user_ref_id})
        if not user:
            raise NotFoundException

        return user.profile_id

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

    def _sync_autopilot_runs(self):
        repository = self.repository
        entities = []
        try:
            for data in self.api.get_autopilot_runs():
                entity = DriveWealthAutopilotRun()
                entity.set_from_response(data)
                entities.append(entity)
                if len(entities) >= MAX_TRANSACTION_SIZE:
                    repository.persist(entities)
        except DriveWealthApiException as e:
            logger.exception(e)
        finally:
            repository.persist(entities)

    def _update_money_flow_status(self, entity: BaseDriveWealthMoneyFlowModel,
                                  money_flow: TradingMoneyFlow):
        money_flow.status = entity.get_money_flow_status()

    def _sync_statements(self, profile_id):
        user = self._get_user(profile_id)
        account = self._get_trading_account(user.ref_id)
        entities: list[DriveWealthStatement] = []
        entities += self.api.get_documents_trading_confirmations(account)
        entities += self.api.get_documents_tax(account)
        entities += self.api.get_documents_statements(account)
        self.repository.persist(entities)
        self.create_trading_statements(entities, profile_id)
