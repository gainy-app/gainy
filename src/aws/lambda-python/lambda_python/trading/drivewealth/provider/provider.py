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

        return repository.upsert_bank_account(data, access_token.id,
                                              account_id)

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
            entity = DriveWealthDeposit(response)
        else:
            response = self.api.create_redemption(amount, trading_account,
                                                  bank_account)
            entity = DriveWealthRedemption(response)

        entity.trading_account_ref_id = trading_account.ref_id
        entity.bank_account_ref_id = bank_account.ref_id
        entity.money_flow_id = money_flow.id
        self._update_money_flow_status(entity, money_flow)
        self.drivewealth_repository.persist(entity)

        self._on_money_transfer(money_flow.profile_id)

        return entity

    def _update_money_flow_status(self, entity, money_flow: TradingMoneyFlow):
        money_flow.status = entity.status
