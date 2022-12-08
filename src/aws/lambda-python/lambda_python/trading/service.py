from typing import Iterable, List
from decimal import Decimal
import io

from exceptions import ValidationException
from gainy.data_access.operators import OperatorIn
from gainy.exceptions import NotFoundException, BadRequestException
from gainy.trading.drivewealth.models import CollectionStatus, CollectionHoldingStatus
from gainy.plaid.common import handle_error
from portfolio.plaid import PlaidService
from services import S3
from trading.drivewealth.provider import DriveWealthProvider
from trading.exceptions import WrongTradingCollectionVersionStatusException
from trading.kyc_form_validator import KycFormValidator
from gainy.plaid.models import PlaidAccessToken, PlaidAccount
from trading.models import KycDocument, TradingMoneyFlow, ProfileKycStatus, TradingStatement

import plaid
from gainy.utils import get_logger, env, ENV_PRODUCTION
from gainy.trading.models import TradingAccount, TradingCollectionVersion, TradingOrderStatus, \
    FundingAccount, TradingOrder, TradingOrderSource
from gainy.trading.service import TradingService as GainyTradingService
from trading.repository import TradingRepository
from verification.models import VerificationCodeChannel

logger = get_logger(__name__)


class TradingService(GainyTradingService):

    def __init__(self, trading_repository: TradingRepository,
                 drivewealth_provider: DriveWealthProvider,
                 plaid_service: PlaidService,
                 kyc_form_validator: KycFormValidator):
        super().__init__(trading_repository, drivewealth_provider,
                         plaid_service)
        self.kyc_form_validator = kyc_form_validator

    def kyc_send_form(self, kyc_form: dict) -> ProfileKycStatus:
        if not kyc_form:
            raise Exception('kyc_form is null')
        return self._get_provider_service().kyc_send_form(kyc_form)

    def kyc_get_status(self, profile_id: int) -> ProfileKycStatus:
        return self._get_provider_service().kyc_get_status(profile_id)

    def send_kyc_document(self, profile_id: int, document: KycDocument):
        document.validate()

        with self.trading_repository.db_conn.cursor() as cursor:
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
        if provider_bank_account.funding_account_id:
            return repository.find_one(
                FundingAccount,
                {"id": provider_bank_account.funding_account_id})

        funding_account = repository.find_one(
            FundingAccount, {
                "plaid_access_token_id": access_token.id,
                "plaid_account_id": account_id
            })
        if funding_account:
            raise BadRequestException('Account already connected.')

        funding_account = FundingAccount()
        funding_account.profile_id = access_token.profile_id
        funding_account.plaid_access_token_id = access_token.id
        funding_account.plaid_account_id = account_id
        funding_account.name = account_name
        repository.persist(funding_account)

        self.update_funding_accounts_balance([funding_account])

        provider_bank_account.funding_account_id = funding_account.id
        repository.persist(provider_bank_account)

        return funding_account

    def filter_existing_funding_accounts(
            self, accounts: List[PlaidAccount]) -> List[PlaidAccount]:
        plaid_account_ids = [i.account_id for i in accounts]
        funding_accounts: List[
            FundingAccount] = self.trading_repository.find_all(
                FundingAccount,
                {"plaid_account_id": OperatorIn(plaid_account_ids)})

        existing_account_ids = [i.plaid_account_id for i in funding_accounts]

        return [
            i for i in accounts if i.account_id not in existing_account_ids
        ]

    def delete_funding_account(self, funding_account: FundingAccount):
        self._get_provider_service().delete_funding_account(funding_account.id)

        repository = self.trading_repository
        repository.delete(funding_account)

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

    # TODO deprecated ?
    def get_actual_collection_holdings(
            self, profile_id, collection_id) -> List[CollectionHoldingStatus]:
        collection_status: CollectionStatus = self._get_provider_service(
        ).get_actual_collection_data(profile_id, collection_id)
        return collection_status.holdings

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

    def validate_kyc_form(self, kyc_form):
        if env() != ENV_PRODUCTION:
            return

        KycFormValidator.validate_address(
            street1=kyc_form['address_street1'],
            street2=kyc_form['address_street2'],
            city=kyc_form['address_city'],
            province=kyc_form['address_province'],
            postal_code=kyc_form['address_postal_code'],
            country=kyc_form['address_country'] or "USA",
        )

        try:
            self.kyc_form_validator.validate_verification(
                profile_id=kyc_form['profile_id'],
                channel=VerificationCodeChannel.SMS,
                address=kyc_form['phone_number'],
            )
        except ValidationException as e:
            logger.info(e)
            raise ValidationException("Phone number %s is not verified." %
                                      kyc_form['phone_number'])

    def cancel_pending_order(
            self, trading_collection_version: TradingCollectionVersion):
        self.trading_repository.refresh(trading_collection_version)
        if trading_collection_version.status != TradingOrderStatus.PENDING:
            raise WrongTradingCollectionVersionStatusException()

        trading_collection_version.status = TradingOrderStatus.CANCELLED
        self.trading_repository.persist(trading_collection_version)

    def download_statement(self, statement: TradingStatement) -> str:
        return self._get_provider_service().download_statement(statement)

    def check_tradeable_symbol(self, symbol: str):
        return self._get_provider_service().check_tradeable_symbol(symbol)

    def create_stock_order(self, profile_id: int, source: TradingOrderSource,
                           symbol: str, trading_account_id: int,
                           target_amount_delta: Decimal):
        self.check_tradeable_symbol(symbol)

        collection_order = TradingOrder()
        collection_order.profile_id = profile_id
        collection_order.source = source
        collection_order.symbol = symbol
        collection_order.status = TradingOrderStatus.PENDING
        collection_order.target_amount_delta = target_amount_delta
        collection_order.trading_account_id = trading_account_id

        self.trading_repository.persist(collection_order)

        return collection_order
