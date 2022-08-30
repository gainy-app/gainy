from managed_portfolio.models import ProfileKycStatus
from managed_portfolio.drivewealth.api import DriveWealthApi
from managed_portfolio.drivewealth.repository import DriveWealthRepository


class DriveWealthProvider:

    def __init__(self):
        self.api = DriveWealthApi()

    def send_kyc_form(self, context_container,
                      kyc_form: dict) -> ProfileKycStatus:
        profile_id = kyc_form['profile_id']
        documents = self._kyc_form_to_documents(kyc_form)

        repository = DriveWealthRepository(context_container)
        user = repository.get_user(profile_id)
        if user is None:
            user_data = self.api.create_user(documents)
        else:
            user_data = self.api.update_user(user['ref_id'], documents)

        data = repository.upsert_user(profile_id, user_data)

        return ProfileKycStatus(data["status"])

    def get_kyc_status(self, context_container,
                       profile_id: int) -> ProfileKycStatus:
        repository = DriveWealthRepository(context_container)
        user = repository.get_user(profile_id)
        if user is None:
            raise Exception("KYC form has not been sent")
        else:
            user_data = self.api.get_user(user['ref_id'])

        data = repository.upsert_user(profile_id, user_data)

        return ProfileKycStatus(data["status"])

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
