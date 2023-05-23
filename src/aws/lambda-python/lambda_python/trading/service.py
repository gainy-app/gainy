from typing import Iterable, List
from decimal import Decimal
import io

from exceptions import ValidationException
from gainy.data_access.operators import OperatorIn
from gainy.exceptions import NotFoundException, BadRequestException
from gainy.trading.drivewealth.models import CollectionStatus, CollectionHoldingStatus
from gainy.plaid.common import handle_error
from gainy.trading.exceptions import InsufficientFundsException, TradingPausedException
from models import UploadedFile
from portfolio.plaid import PlaidService
from services.uploaded_file_service import UploadedFileService
from trading.drivewealth.provider import DriveWealthProvider
from trading.exceptions import WrongTradingOrderStatusException, CannotDeleteFundingAccountException
from trading.kyc_form_validator import KycFormValidator
from gainy.plaid.models import PlaidAccessToken, PlaidAccount
from trading.models import KycDocument

import plaid
from gainy.utils import get_logger, env, ENV_PRODUCTION
from gainy.trading.models import TradingAccount, TradingCollectionVersion, TradingOrderStatus, \
    FundingAccount, TradingOrder, TradingMoneyFlowStatus, TradingMoneyFlow, AbstractProviderBankAccount, \
    ProfileKycStatus, KycStatus, TradingStatement
from gainy.trading.service import TradingService as GainyTradingService
from trading.repository import TradingRepository
from verification.models import VerificationCodeChannel

logger = get_logger(__name__)


class TradingService(GainyTradingService):
    trading_repository: TradingRepository

    def __init__(self, trading_repository: TradingRepository,
                 drivewealth_provider: DriveWealthProvider,
                 plaid_service: PlaidService,
                 kyc_form_validator: KycFormValidator,
                 file_service: UploadedFileService):
        super().__init__(trading_repository, drivewealth_provider,
                         plaid_service)
        self.kyc_form_validator = kyc_form_validator
        self.file_service = file_service

    def kyc_send_form(self, kyc_form: dict) -> ProfileKycStatus:
        if not kyc_form:
            raise Exception('kyc_form is null')
        return self._get_provider_service().kyc_send_form(kyc_form)

    def send_kyc_document(self, profile_id: int, document: KycDocument):
        document.validate()

        uploaded_file: UploadedFile = self.trading_repository.find_one(
            UploadedFile, {
                "profile_id": profile_id,
                "id": document.uploaded_file_id,
            })
        if not uploaded_file:
            raise NotFoundException('File not Found')
        document.content_type = uploaded_file.content_type
        document.profile_id = profile_id

        self.trading_repository.persist(document)

        file_stream = io.BytesIO()
        self.file_service.download_to_stream(uploaded_file, file_stream)

        return self._get_provider_service().send_kyc_document(
            profile_id, document, file_stream)

    def link_bank_account_with_plaid(self, access_token: PlaidAccessToken,
                                     account_name,
                                     account_id) -> FundingAccount:
        """
        :raises TradingPausedException:
        """
        self.trading_repository.check_profile_trading_not_paused(
            access_token.profile_id)
        try:
            provider_bank_account: AbstractProviderBankAccount = self._get_provider_service(
            ).link_bank_account_with_plaid(access_token, account_id,
                                           account_name)
        except plaid.ApiException as e:
            return handle_error(e)

        repository = self.trading_repository
        if provider_bank_account.funding_account_id:
            return repository.find_one(
                FundingAccount,
                {"id": provider_bank_account.funding_account_id})

        funding_account = FundingAccount()
        funding_account.profile_id = access_token.profile_id
        funding_account.plaid_access_token_id = access_token.id
        funding_account.plaid_account_id = account_id
        funding_account.name = account_name
        provider_bank_account.fill_funding_account_details(funding_account)

        existing_funding_accounts: Iterable[
            FundingAccount] = repository.find_all(
                FundingAccount, {
                    "profile_id": funding_account.profile_id,
                    "mask": funding_account.mask,
                    "name": funding_account.name,
                })
        for fa in existing_funding_accounts:
            at: PlaidAccessToken = repository.find_one(
                PlaidAccessToken, {
                    "id": fa.plaid_access_token_id,
                })

            if at and at.institution_id == access_token.institution_id:
                raise BadRequestException('Account already connected.')

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
        """
        :raises TradingPausedException:
        """
        self.trading_repository.check_profile_trading_not_paused(
            funding_account.profile_id)
        money_flow = self.trading_repository.find_one(
            TradingMoneyFlow, {
                "funding_account_id":
                funding_account.id,
                "status":
                OperatorIn([
                    TradingMoneyFlowStatus.PENDING.name,
                    TradingMoneyFlowStatus.APPROVED.name
                ]),
            })
        if money_flow:
            raise CannotDeleteFundingAccountException(
                "You can not delete an account while it has pending deposits or withdrawals."
            )

        self._get_provider_service().delete_funding_account(funding_account.id)

        repository = self.trading_repository
        repository.delete(funding_account)

    def create_money_flow(self, profile_id: int, amount: Decimal,
                          trading_account: TradingAccount,
                          funding_account: FundingAccount):
        """
        :raises InsufficientFundsException:
        :raises TradingPausedException:
        """
        repository = self.trading_repository

        repository.check_profile_trading_not_paused(profile_id)

        if amount > 0:
            self.check_enough_funds_to_deposit(funding_account, amount)
        else:
            self.check_enough_withdrawable_cash(trading_account.id, -amount)

        money_flow = TradingMoneyFlow()
        money_flow.profile_id = profile_id
        money_flow.status = TradingMoneyFlowStatus.PENDING
        money_flow.amount = amount
        money_flow.trading_account_id = trading_account.id
        money_flow.funding_account_id = funding_account.id
        repository.persist(money_flow)

        try:
            self._get_provider_service().transfer_money(
                money_flow, amount, trading_account.id, funding_account.id)
        except Exception as e:
            money_flow.status = TradingMoneyFlowStatus.FAILED
            raise e
        finally:
            repository.persist(money_flow)

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

        self.kyc_form_validator.validate_address(
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

    def cancel_pending_collection_version(
            self, trading_collection_version: TradingCollectionVersion):
        self.trading_repository.refresh(trading_collection_version)
        if trading_collection_version.status != TradingOrderStatus.PENDING:
            raise WrongTradingOrderStatusException()

        trading_collection_version.status = TradingOrderStatus.CANCELLED
        self.trading_repository.persist(trading_collection_version)

    def cancel_pending_order(self, trading_order: TradingOrder):
        self.trading_repository.refresh(trading_order)
        if trading_order.status != TradingOrderStatus.PENDING:
            raise WrongTradingOrderStatusException()

        trading_order.status = TradingOrderStatus.CANCELLED
        self.trading_repository.persist(trading_order)

    def download_statement(self, statement: TradingStatement) -> str:
        return self._get_provider_service().download_statement(statement)

    def handle_kyc_status_change(self, status: ProfileKycStatus):
        if status.status == KycStatus.APPROVED:
            self.remove_sensitive_kyc_data(status.profile_id)
            self.trading_repository.remove_sensitive_kyc_data(
                status.profile_id)

    def remove_sensitive_kyc_data(self, profile_id):
        # remove documents from s3
        for document in self.trading_repository.iterate_all(
                KycDocument, {"profile_id": profile_id}):
            document: KycDocument
            if not document.uploaded_file_id:
                continue

            uploaded_file: UploadedFile = self.trading_repository.find_one(
                UploadedFile, {"id": document.uploaded_file_id})
            if not uploaded_file:
                continue

            self.file_service.remove_file(uploaded_file)
            self.trading_repository.delete(document)
            self.trading_repository.delete(uploaded_file)

    def check_enough_funds_to_deposit(self, funding_account: FundingAccount,
                                      amount: Decimal):
        """
        :raises InsufficientFundsException:
        """
        try:
            self.update_funding_accounts_balance([funding_account])
        except Exception as e:
            logger.exception(
                'Exception while updating funding account balance',
                extra={
                    "e": e,
                    "profile_id": funding_account.profile_id,
                    "funding_account_id": funding_account.id,
                })

        logger.info('check_enough_funds_to_deposit',
                    extra={
                        "profile_id": funding_account.profile_id,
                        "funding_account": funding_account.to_dict(),
                    })
        if funding_account.balance is None or Decimal(
                funding_account.balance) < amount:
            raise InsufficientFundsException()
