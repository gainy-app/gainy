import base64
import io
from trading.models import ProfileKycStatus, KycDocument, TradingAccount
from trading.drivewealth.models import DriveWealthAccount
from trading.drivewealth.api import DriveWealthApi
from trading.drivewealth.repository import DriveWealthRepository
from gainy.utils import get_logger

logger = get_logger(__name__)


class DriveWealthProviderKYC:
    drivewealth_repository: DriveWealthRepository = None
    api: DriveWealthApi = None

    def __init__(self, drivewealth_repository: DriveWealthRepository,
                 api: DriveWealthApi):
        self.drivewealth_repository = drivewealth_repository
        self.api = api

    def kyc_send_form(self, kyc_form: dict) -> ProfileKycStatus:
        profile_id = kyc_form['profile_id']
        documents = self._kyc_form_to_documents(kyc_form)

        # create or update user
        repository = self.drivewealth_repository
        user = repository.get_user(profile_id)
        if user is None:
            user_data = self.api.create_user(documents)
        else:
            user_data = self.api.update_user(user.ref_id, documents)

        user = repository.upsert_user(profile_id, user_data)
        user_ref_id = user.ref_id

        # create or update account
        accounts = repository.get_user_accounts(user_ref_id)
        if not accounts:
            account_data = self.api.create_account(user_ref_id)
            repository.upsert_user_account(user_ref_id, account_data)

        return self.api.get_kyc_status(user_ref_id).get_profile_kyc_status()

    def kyc_get_status(self, profile_id: int) -> ProfileKycStatus:
        repository = self.drivewealth_repository
        user = repository.get_user(profile_id)
        if user is None:
            raise Exception("KYC form has not been sent")
        else:
            user_data = self.api.get_user(user.ref_id)

        user = repository.upsert_user(profile_id, user_data)
        user_ref_id = user.ref_id

        kyc_status = self.api.get_kyc_status(
            user_ref_id).get_profile_kyc_status()

        documents_data = self.api.get_user_documents(user_ref_id)
        for document_data in documents_data:
            repository.upsert_kyc_document(None, document_data)

        accounts_data = self.api.get_user_accounts(user_ref_id)
        for account_data in accounts_data:
            drivewealth_trading_account = repository.upsert_user_account(
                user_ref_id, account_data)
            drivewealth_trading_account = repository.find_one(
                DriveWealthAccount,
                {"ref_id": drivewealth_trading_account.ref_id})

            if drivewealth_trading_account.trading_account_id is None:
                # TODO move to somewhere more general
                trading_account = TradingAccount()
                trading_account.profile_id = profile_id
                trading_account.name = drivewealth_trading_account.nickname
            else:
                trading_account = repository.find_one(
                    TradingAccount,
                    {"id": drivewealth_trading_account.trading_account_id})

            trading_account.cash_available_for_trade = drivewealth_trading_account.cash_available_for_trade
            trading_account.cash_available_for_withdrawal = drivewealth_trading_account.cash_available_for_withdrawal
            trading_account.cash_balance = drivewealth_trading_account.cash_balance
            repository.persist(trading_account)
            drivewealth_trading_account.trading_account_id = trading_account.id
            repository.persist(drivewealth_trading_account)

        return kyc_status

    def send_kyc_document(self, profile_id: int, document: KycDocument,
                          file_stream: io.BytesIO):
        file_base64 = base64.b64encode(file_stream.getvalue())

        file_data = f"data:{document.content_type};base64,{file_base64}"

        repository = self.drivewealth_repository
        user = repository.get_user(profile_id)
        if user is None:
            raise Exception("KYC form has not been sent")

        data = self.api.upload_document(user.ref_id, document, file_data)
        repository.upsert_kyc_document(document.id, data)

    def _kyc_form_to_documents(self, kyc_form: dict):
        return [
            {
                "type": "BASIC_INFO",
                "data": {
                    "firstName": kyc_form['first_name'],
                    "lastName": kyc_form['last_name'],
                    "country": kyc_form['country'] or "USA",
                    "phone": kyc_form['phone_number'],
                    "emailAddress": kyc_form['email_address'],
                    "language": kyc_form['language'] or "en_US"
                }
            },
            {
                "type": "EMPLOYMENT_INFO",
                "data": {
                    "status":
                    kyc_form['employment_status'],
                    "company":
                    kyc_form['employment_company_name'],
                    "type":
                    kyc_form['employment_type'],
                    "position":
                    kyc_form['employment_position'],
                    "broker":
                    kyc_form['employment_affiliated_with_a_broker'],
                    "directorOf":
                    kyc_form['employment_is_director_of_a_public_company']
                }
            },
            {
                "type": "INVESTOR_PROFILE_INFO",
                "data": {
                    "investmentExperience":
                    kyc_form['investor_profile_experience'],
                    "annualIncome":
                    kyc_form['investor_profile_annual_income'],
                    "networthTotal":
                    kyc_form['investor_profile_net_worth_total'],
                    "riskTolerance":
                    kyc_form['investor_profile_risk_tolerance'],
                    "investmentObjectives":
                    kyc_form['investor_profile_objectives'],
                    "networthLiquid":
                    kyc_form['investor_profile_net_worth_liquid'],
                }
            },
            {
                "type": "DISCLOSURES",
                "data": {
                    "termsOfUse":
                    kyc_form['disclosures_drivewealth_terms_of_use'],
                    "customerAgreement":
                    kyc_form['disclosures_drivewealth_customer_agreement'],
                    "iraAgreement":
                    kyc_form['disclosures_drivewealth_ira_agreement'],
                    "marketDataAgreement":
                    kyc_form['disclosures_drivewealth_market_data_agreement'],
                    "rule14b":
                    kyc_form['disclosures_rule14b'],
                    "findersFee":
                    False,
                    "privacyPolicy":
                    kyc_form['disclosures_drivewealth_privacy_policy'],
                    "dataSharing":
                    kyc_form['disclosures_drivewealth_data_sharing'],
                    "signedBy":
                    kyc_form['disclosures_signed_by'],
                    "extendedHoursAgreement":
                    kyc_form['disclosures_extended_hours_agreement'],
                }
            },
            {
                "type": "IDENTIFICATION_INFO",
                "data": {
                    "value":
                    kyc_form['tax_id_value'].replace('-', '')
                    if 'tax_id_value' in kyc_form and kyc_form['tax_id_value']
                    else None,
                    "type":
                    kyc_form['tax_id_type'],
                    "citizenship":
                    kyc_form['citizenship'] or "USA",
                    "usTaxPayer":
                    kyc_form['is_us_tax_payer'],
                }
            },
            {
                "type": "TAX_INFO",
                "data": {
                    "taxTreatyWithUS": kyc_form['tax_treaty_with_us'],
                }
            },
            {
                "type": "PERSONAL_INFO",
                "data": {
                    "birthDay":
                    kyc_form['birthdate'].day
                    if kyc_form['birthdate'] else None,
                    "birthMonth":
                    kyc_form['birthdate'].month
                    if kyc_form['birthdate'] else None,
                    "birthYear":
                    kyc_form['birthdate'].year
                    if kyc_form['birthdate'] else None,
                    "politicallyExposedNames":
                    kyc_form['politically_exposed_names'],
                    "irsBackupWithholdings":
                    kyc_form['irs_backup_withholdings_notified'],
                    "gender":
                    kyc_form['gender'],
                    "marital":
                    kyc_form['marital_status'],
                }
            },
            {
                "type": "ADDRESS_INFO",
                "data": {
                    "street1": kyc_form['address_street1'],
                    "street2": kyc_form['address_street2'],
                    "city": kyc_form['address_city'],
                    "province": kyc_form['address_province'],
                    "postalCode": kyc_form['address_postal_code'],
                    "country": kyc_form['address_country'],
                }
            },
        ]