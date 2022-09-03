from typing import Iterable
import io
import plaid

from common.exceptions import NotFoundException
from portfolio.plaid import PlaidService
from portfolio.plaid.common import handle_error
from services import S3
from portfolio.plaid.models import PlaidAccessToken
from managed_portfolio.models import KycDocument, ManagedPortfolioFundingAccount
from managed_portfolio.drivewealth import DriveWealthProvider
from managed_portfolio.repository import ManagedPortfolioRepository
from gainy.utils import get_logger
from gainy.data_access.repository import Repository

logger = get_logger(__name__)


class ManagedPortfolioService:

    def __init__(self, db_conn,
                 managed_portfolio_repository: ManagedPortfolioRepository,
                 drivewealth_provider: DriveWealthProvider,
                 plaid_service: PlaidService):
        self.db_conn = db_conn
        self.managed_portfolio_repository = managed_portfolio_repository
        self.drivewealth_provider = drivewealth_provider
        self.plaid_service = plaid_service

    def send_kyc_form(self, kyc_form: dict):
        if not kyc_form:
            raise Exception('kyc_form is null')
        return self.get_provider_service().send_kyc_form(kyc_form)

    def get_kyc_status(self, profile_id: int):
        return self.get_provider_service().get_kyc_status(profile_id)

    def send_kyc_document(self, profile_id: int, document: KycDocument):
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

        self.managed_portfolio_repository.upsert_kyc_document(
            profile_id, document)

        file_stream = io.BytesIO()
        S3().download_file(s3_bucket, s3_key, file_stream)

        return self.get_provider_service().send_kyc_document(
            profile_id, document, file_stream)

    def link_bank_account_with_plaid(
            self, access_token, account_name,
            account_id) -> ManagedPortfolioFundingAccount:
        try:
            provider_bank_account = self.get_provider_service(
            ).link_bank_account_with_plaid(access_token, account_id,
                                           account_name)
        except plaid.ApiException as e:
            handle_error(e)

        repository = self.managed_portfolio_repository
        funding_account = repository.find_one(
            ManagedPortfolioFundingAccount,
            {"plaid_access_token_id": access_token['id']})
        if not funding_account:
            funding_account = ManagedPortfolioFundingAccount()
            funding_account.profile_id = access_token['profile_id']
            funding_account.plaid_access_token_id = access_token['id']
            funding_account.plaid_account_id = account_id
            funding_account.name = account_name

            repository.persist(funding_account)

        provider_bank_account.managed_portfolio_funding_account_id = funding_account.id
        logger.info(provider_bank_account.to_dict())
        repository.persist(provider_bank_account)

        return funding_account

    def update_funding_accounts_balance(
            self, funding_accounts: Iterable[ManagedPortfolioFundingAccount]):
        by_at_id = {}
        for funding_account in funding_accounts:
            if not funding_account.plaid_access_token_id:
                continue
            if funding_account.plaid_access_token_id not in by_at_id:
                by_at_id[funding_account.plaid_access_token_id] = []
            by_at_id[funding_account.plaid_access_token_id].append(
                funding_account)

        plaid_service = self.plaid_service
        repository = self.managed_portfolio_repository
        for plaid_access_token_id, funding_accounts in by_at_id.items():
            access_token = repository.find_one(PlaidAccessToken,
                                               {"id": plaid_access_token_id})
            funding_accounts_by_account_id = {
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

            repository.persist(funding_account)

    def delete_funding_account(
            self, funding_account: ManagedPortfolioFundingAccount):
        self.get_provider_service().delete_funding_account(funding_account.id)

        repository = self.managed_portfolio_repository
        repository.delete(funding_account)
        repository.delete_by(PlaidAccessToken,
                             {"id": funding_account.plaid_access_token_id})

    def get_provider_service(self):
        return self.drivewealth_provider