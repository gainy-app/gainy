from functools import lru_cache
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from common import make_graphql_request

PROFILES = make_graphql_request("{app_profiles{id, user_id}}",
                                user_id=None)['data']['app_profiles']


@lru_cache(maxsize=None)
def load_query(directory, query_name):
    query_file = os.path.join(os.path.dirname(__file__), 'queries', directory,
                              query_name + '.graphql')
    with open(query_file, 'r') as f:
        return f.read()


def fill_kyc_form(profile_id, profile_user_id):
    kyc_form_config = make_graphql_request(
        load_query('kyc', 'GetKycFormConfig'), {"profile_id": profile_id},
        profile_user_id)['data']['get_kyc_form_config']

    signed_by = " ".join([
        kyc_form_config["first_name"]["placeholder"],
        kyc_form_config["last_name"]["placeholder"]
    ])

    data = {
        "profile_id": profile_id,
        "first_name": kyc_form_config["first_name"]["placeholder"],
        "last_name": kyc_form_config["last_name"]["placeholder"],
        "email_address": kyc_form_config["email_address"]["placeholder"],
        "phone_number": "+1234567890",
        "birthdate": "1992-11-27",
        "address_street1": "1 Wall st.",
        "address_city": "New York",
        "address_postal_code": "12345",
        "tax_id_value": "123456789",
        "tax_id_type": "SSN",
        "employment_status": "UNEMPLOYED",
        "investor_profile_annual_income": 123456,
        "investor_profile_objectives": "LONG_TERM",
        "investor_profile_experience": "YRS_10_",
        "investor_profile_net_worth_liquid": 123,
        "investor_profile_net_worth_total": 1234,
        "investor_profile_risk_tolerance": "SPECULATION",
        "disclosures_drivewealth_terms_of_use": True,
        "disclosures_rule14b": True,
        "disclosures_drivewealth_customer_agreement": True,
        "disclosures_drivewealth_privacy_policy": True,
        "disclosures_drivewealth_market_data_agreement": True,
        "disclosures_signed_by": signed_by
    }

    make_graphql_request(
        load_query('kyc', 'UpsertKycForm'), data,
        profile_user_id)['data']['insert_app_kyc_form']['returning'][0]
    make_graphql_request(
        load_query('kyc', 'UpsertKycForm'), data,
        profile_user_id)['data']['insert_app_kyc_form']['returning'][0]


def send_kyc_form(profile_id, profile_user_id):
    response = make_graphql_request(load_query('kyc', 'SendKycForm'),
                                    {"profile_id": profile_id},
                                    profile_user_id)['data']['send_kyc_form']

    assert response.get("error_message") is None
    assert response.get("status") is not None
