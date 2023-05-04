from decimal import Decimal

from gainy.analytics.service import AnalyticsService
from gainy.data_access.repository import MAX_TRANSACTION_SIZE
from gainy.exceptions import NotFoundException, EntityNotFoundException
from gainy.trading.drivewealth.config import DRIVEWEALTH_IS_UAT
from gainy.trading.drivewealth.exceptions import DriveWealthApiException
from portfolio.plaid import PlaidService
from gainy.plaid.models import PlaidAccessToken
from gainy.services.notification import NotificationService
from trading.drivewealth.provider.collection import DriveWealthProviderCollection
from trading.drivewealth.provider.kyc import DriveWealthProviderKYC
from trading.drivewealth.models import DriveWealthAutopilotRun
from trading.drivewealth.api import DriveWealthApi
from trading.drivewealth.repository import DriveWealthRepository

from gainy.utils import get_logger
from gainy.trading.models import FundingAccount, TradingAccount, TradingCollectionVersion, TradingMoneyFlowStatus, \
    TradingMoneyFlow, ProfileKycStatus, TradingStatement, KycForm
from gainy.trading.drivewealth.models import DriveWealthAccount, DriveWealthUser, \
    DriveWealthTransaction, \
    DriveWealthBankAccount, DriveWealthDeposit, DriveWealthRedemption, DriveWealthStatement, DriveWealthOrder
from trading.repository import TradingRepository

logger = get_logger(__name__)


class DriveWealthProvider(DriveWealthProviderKYC,
                          DriveWealthProviderCollection):

    def __init__(self,
                 repository: DriveWealthRepository,
                 api: DriveWealthApi,
                 trading_repository: TradingRepository,
                 plaid_service: PlaidService,
                 notification_service: NotificationService,
                 analytics_service: AnalyticsService = None):
        super().__init__(repository, api, trading_repository,
                         notification_service, analytics_service)
        self.plaid_service = plaid_service

    def link_bank_account_with_plaid(
            self, access_token: PlaidAccessToken, account_id: str,
            account_name: str) -> DriveWealthBankAccount:
        repository = self.repository
        bank_account = repository.find_one(
            DriveWealthBankAccount, {
                "plaid_access_token_id": access_token.id,
                "plaid_account_id": account_id
            })
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
        if not drivewealth_bank_account:
            return
        self.api.delete_bank_account(drivewealth_bank_account.ref_id)
        self.repository.delete(drivewealth_bank_account)

    def transfer_money(self, money_flow: TradingMoneyFlow, amount: Decimal,
                       trading_account_id: int, funding_account_id: int):
        trading_account = self.repository.find_one(
            DriveWealthAccount, {"trading_account_id": trading_account_id})
        if not trading_account:
            raise EntityNotFoundException(DriveWealthAccount)

        bank_account = self.repository.find_one(
            DriveWealthBankAccount, {"funding_account_id": funding_account_id})
        if not bank_account:
            raise EntityNotFoundException(DriveWealthBankAccount)

        try:
            if amount > 0:
                entity = self.api.create_deposit(amount, trading_account,
                                                 bank_account)
            else:
                entity = self.api.create_redemption(amount, trading_account,
                                                    bank_account)
        except DriveWealthApiException as e:
            money_flow.status = TradingMoneyFlowStatus.FAILED
            self.repository.persist(money_flow)
            logger.exception(e)
            raise Exception('Request failed, please try again later.')

        entity.trading_account_ref_id = trading_account.ref_id
        entity.bank_account_ref_id = bank_account.ref_id
        entity.money_flow_id = money_flow.id
        self.update_money_flow_from_dw(entity, money_flow)
        self.repository.persist(entity)

        if amount > 0:
            logger.info("Created deposit",
                        extra={
                            "file": __file__,
                            "deposit": entity.to_dict(),
                        })
        else:
            logger.info("Created redemption",
                        extra={
                            "file": __file__,
                            "redemption": entity.to_dict(),
                        })

    def debug_add_money(self, trading_account_id, amount):
        if not DRIVEWEALTH_IS_UAT:
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
        if not DRIVEWEALTH_IS_UAT:
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

        for entity in repository.find_all(ProfileKycStatus,
                                          {"profile_id": profile_id}):
            repository.delete(entity)

        entity: KycForm = repository.find_one(KycForm,
                                              {"profile_id": profile_id})
        if entity:
            entity.status = None
            repository.persist(entity)

    def sync_data(self, profile_id):
        user = self._get_user(profile_id)

        self.sync_user(user.ref_id)
        self.sync_kyc(user.ref_id)
        self.sync_profile_trading_accounts(profile_id)
        self._sync_autopilot_runs()
        self._sync_bank_accounts(user.ref_id)
        self._sync_user_deposits(user.ref_id)
        self._sync_user_redemptions(user.ref_id)
        self.sync_portfolios(profile_id)
        self._sync_transactions(user.ref_id)
        self._sync_statements(profile_id)

    def sync_deposit(self, deposit_ref_id: str):
        repository = self.repository

        entity = repository.find_one(
            DriveWealthDeposit,
            {"ref_id": deposit_ref_id}) or DriveWealthDeposit()
        deposit_pre = entity.to_dict()

        deposit_data = self.api.get_deposit(deposit_ref_id)
        entity.set_from_response(deposit_data)
        self.ensure_account_exists(entity.trading_account_ref_id)
        repository.persist(entity)

        logger.info("Updated deposit",
                    extra={
                        "file": __file__,
                        "deposit_pre": deposit_pre,
                        "deposit": entity.to_dict(),
                    })

        self.update_money_flow_from_dw(entity)

    def sync_autopilot_run(self, entity: DriveWealthAutopilotRun):
        repository = self.repository
        try:
            data = self.api.get_autopilot_run(entity.ref_id)
        except DriveWealthApiException as e:
            logger.warning(e)
            return

        entity.set_from_response(data)
        repository.persist(entity)

    def download_statement(self, statement: TradingStatement) -> str:
        dw_statement = self.repository.find_one(
            DriveWealthStatement, {"trading_statement_id": statement.id})
        if not dw_statement:
            raise NotFoundException

        return self.api.get_statement_url(dw_statement)

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
            deposit_pre = entity.to_dict()
            entity.set_from_response(deposit_data)
            repository.persist(entity)

            logger.info("Updated deposit",
                        extra={
                            "file": __file__,
                            "deposit_pre": deposit_pre,
                            "deposit": entity.to_dict(),
                        })

            self.update_money_flow_from_dw(entity)

    def _sync_user_redemptions(self, user_ref_id: str):
        repository = self.repository

        redemptions_data = self.api.get_user_redemptions(user_ref_id)
        for redemption_data in redemptions_data:
            entity = repository.find_one(
                DriveWealthRedemption,
                {"ref_id": redemption_data['id']}) or DriveWealthRedemption()
            redemption_pre = entity.to_dict()
            entity.set_from_response(redemption_data)
            repository.persist(entity)

            logger.info("Updated redemption",
                        extra={
                            "file": __file__,
                            "redemption_pre": redemption_pre,
                            "redemption": entity.to_dict(),
                        })

            self.update_money_flow_from_dw(entity)

    def _sync_transactions(self, user_ref_id: str):
        for account in self.repository.iterate_all(
                DriveWealthAccount, {"drivewealth_user_id": user_ref_id}):
            account: DriveWealthAccount
            if not account.is_open():
                continue

            for data in self.api.iterate_user_transactions(account):
                order_id = data.get("orderID")
                if not order_id or order_id == "0":
                    transaction = DriveWealthTransaction()
                    transaction.account_id = account.ref_id
                    transaction.set_from_response(data)
                    self.repository.persist(transaction)
                    continue

                order: DriveWealthOrder = self.repository.find_one(
                    DriveWealthOrder, {"ref_id": order_id})
                if order and order.is_filled(
                ) and order.last_executed_at and order.total_order_amount_normalized:
                    continue
                if order and order.is_rejected():
                    continue

                data = self.api.get_order(order_id)
                if not order:
                    order = DriveWealthOrder()
                order.set_from_response(data)
                self.repository.persist(order)

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

    def _sync_statements(self, profile_id):
        user = self._get_user(profile_id)
        account = self._get_trading_account(user.ref_id)
        entities: list[DriveWealthStatement] = []
        entities += self.api.get_documents_trading_confirmations(account)
        entities += self.api.get_documents_tax(account)
        entities += self.api.get_documents_statements(account)

        for i in entities:
            self.repository.refresh(i)

        self.repository.persist(entities)
        self.create_trading_statements(entities, profile_id)
