from typing import Iterable, Dict, List
from decimal import Decimal
import io

from gainy.exceptions import NotFoundException
from portfolio.plaid import PlaidService
from portfolio.plaid.common import handle_error
from services import S3
from portfolio.plaid.models import PlaidAccessToken
from trading.models import KycDocument, FundingAccount, TradingMoneyFlow, TradingCollectionVersion, \
    CollectionHoldingStatus, ProfileKycStatus, CollectionStatus, ProfileBalances, TradingCollectionVersionStatus
from trading.drivewealth.provider import DriveWealthProvider
from trading.repository import TradingRepository

import plaid
from gainy.utils import get_logger
from gainy.trading.models import TradingAccount
from gainy.trading import TradingService as GainyTradingService

logger = get_logger(__name__)


class TradingService(GainyTradingService):

    def __init__(self, db_conn, trading_repository: TradingRepository,
                 drivewealth_provider: DriveWealthProvider,
                 plaid_service: PlaidService):
        self.db_conn = db_conn
        self.trading_repository = trading_repository
        self.drivewealth_provider = drivewealth_provider
        self.plaid_service = plaid_service

    def kyc_send_form(self, kyc_form: dict):
        if not kyc_form:
            raise Exception('kyc_form is null')
        return self._get_provider_service().kyc_send_form(kyc_form)

    def kyc_get_status(self, profile_id: int) -> ProfileKycStatus:
        return self._get_provider_service().kyc_get_status(profile_id)

    def send_kyc_document(self, profile_id: int, document: KycDocument):
        document.validate()

        with self.db_conn.cursor() as cursor:
            cursor.execute(
                """select s3_bucket, s3_key, content_type from app.uploaded_files
                where profile_id = %(profile_id)s and id = %(id)s""", {
                    "profile_id": profile_id,
                    "id": document.uploaded_file_id,
                })
            row = cursor.fetchone()

        if row is None:
            raise NotFoundException('File not Found')
        (s3_bucket, s3_key, content_type) = row
        document.content_type = content_type
        document.profile_id = profile_id

        self.trading_repository.persist(document)

        file_stream = io.BytesIO()
        S3().download_file(s3_bucket, s3_key, file_stream)

        return self._get_provider_service().send_kyc_document(
            profile_id, document, file_stream)

    def link_bank_account_with_plaid(self, access_token: PlaidAccessToken,
                                     account_name,
                                     account_id) -> FundingAccount:
        try:
            provider_bank_account = self._get_provider_service(
            ).link_bank_account_with_plaid(access_token, account_id,
                                           account_name)
        except plaid.ApiException as e:
            return handle_error(e)

        repository = self.trading_repository
        funding_account = repository.find_one(
            FundingAccount, {"plaid_access_token_id": access_token.id})
        if not funding_account:
            funding_account = FundingAccount()
            funding_account.profile_id = access_token.profile_id
            funding_account.plaid_access_token_id = access_token.id
            funding_account.plaid_account_id = account_id
            funding_account.name = account_name

            repository.persist(funding_account)

        provider_bank_account.funding_account_id = funding_account.id
        logger.info(provider_bank_account.to_dict())
        repository.persist(provider_bank_account)

        return funding_account

    def update_funding_accounts_balance(
            self, funding_accounts: Iterable[FundingAccount]):
        by_at_id = {}
        for funding_account in funding_accounts:
            if not funding_account.plaid_access_token_id:
                continue
            if funding_account.plaid_access_token_id not in by_at_id:
                by_at_id[funding_account.plaid_access_token_id] = []
            by_at_id[funding_account.plaid_access_token_id].append(
                funding_account)

        plaid_service = self.plaid_service
        repository = self.trading_repository
        for plaid_access_token_id, funding_accounts in by_at_id.items():
            access_token = repository.find_one(PlaidAccessToken,
                                               {"id": plaid_access_token_id})
            funding_accounts_by_account_id: Dict[int, FundingAccount] = {
                funding_account.plaid_account_id: funding_account
                for funding_account in funding_accounts
                if funding_account.plaid_account_id
            }

            plaid_accounts = plaid_service.get_item_accounts(
                access_token.access_token,
                list(funding_accounts_by_account_id.keys()))
            for plaid_account in plaid_accounts:
                if plaid_account.account_id not in funding_accounts_by_account_id:
                    continue
                funding_accounts_by_account_id[
                    plaid_account.
                    account_id].balance = plaid_account.balance_available

            repository.persist(funding_accounts_by_account_id.values())

    def delete_funding_account(self, funding_account: FundingAccount):
        self._get_provider_service().delete_funding_account(funding_account.id)

        repository = self.trading_repository
        repository.delete(funding_account)
        repository.delete_by(PlaidAccessToken,
                             {"id": funding_account.plaid_access_token_id})

    def create_money_flow(self, profile_id: int, amount: Decimal,
                          trading_account: TradingAccount,
                          funding_account: FundingAccount):
        repository = self.trading_repository

        money_flow = TradingMoneyFlow()
        money_flow.profile_id = profile_id
        money_flow.amount = amount
        money_flow.trading_account_id = trading_account.id
        money_flow.funding_account_id = funding_account.id
        repository.persist(money_flow)

        self._get_provider_service().transfer_money(money_flow, amount,
                                                    trading_account.id,
                                                    funding_account.id)

        return money_flow

    def reconfigure_collection_holdings(self, profile_id: int,
                                        collection_id: int,
                                        weights: Dict[str, Decimal],
                                        target_amount_delta: Decimal):
        collection_version = TradingCollectionVersion()
        collection_version.status = TradingCollectionVersionStatus.PENDING
        collection_version.profile_id = profile_id
        collection_version.collection_id = collection_id
        collection_version.weights = weights
        collection_version.target_amount_delta = target_amount_delta

        self.trading_repository.persist(collection_version)

        self._get_provider_service().reconfigure_collection_holdings(
            collection_version)

        return collection_version

    # TODO deprecated ?
    def get_actual_collection_holdings(
            self, profile_id, collection_id) -> List[CollectionHoldingStatus]:
        collection_status: CollectionStatus = self._get_provider_service(
        ).get_actual_collection_data(profile_id, collection_id)
        return collection_status.holdings

    def get_actual_collection_status(self, profile_id: int,
                                     collection_id: int) -> CollectionStatus:
        return self._get_provider_service().get_actual_collection_data(
            profile_id, collection_id)

    def get_actual_balances(self, profile_id: int) -> ProfileBalances:
        return self._get_provider_service().get_actual_balances(profile_id)

    def sync_funding_accounts(self, profile_id) -> Iterable[FundingAccount]:
        repository = self.trading_repository
        funding_accounts = repository.find_all(FundingAccount,
                                               {"profile_id": profile_id})
        self.update_funding_accounts_balance(funding_accounts)

        return funding_accounts

    def sync_provider_data(self, profile_id):
        self.sync_funding_accounts(profile_id)
        self._get_provider_service().sync_data(profile_id)

    def debug_add_money(self, trading_account_id, amount):
        self._get_provider_service().debug_add_money(trading_account_id,
                                                     amount)

    def debug_delete_data(self, profile_id):
        self._get_provider_service().debug_delete_data(profile_id)
