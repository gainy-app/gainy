import os
from common import make_graphql_request, db_connect

PROFILES = make_graphql_request("{app_profiles{id, user_id}}",
                                user_id=None)['data']['app_profiles']

query_names = [
    'UpsertKycForm',
    'GetKycForm',
    'GetKycFormConfig',
    'SendKycForm',
    'GetKycStatus',
]
QUERIES = {}
for query_name in query_names:
    query_file = os.path.join(os.path.dirname(__file__), 'queries',
                              query_name + '.graphql')
    with open(query_file, 'r') as f:
        QUERIES[query_name] = f.read()


def test_upsert_kyc_form():
    profile_id = PROFILES[0]['id']
    profile_user_id = PROFILES[0]['user_id']

    kyc_form_config = make_graphql_request(
        QUERIES['GetKycFormConfig'], {"profile_id": profile_id},
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
        QUERIES['UpsertKycForm'], data,
        profile_user_id)['data']['insert_app_kyc_form']['returning'][0]
    make_graphql_request(
        QUERIES['UpsertKycForm'], data,
        profile_user_id)['data']['insert_app_kyc_form']['returning'][0]


def test_get_kyc_form():
    profile_id = PROFILES[0]['id']
    profile_user_id = PROFILES[0]['user_id']

    make_graphql_request(QUERIES['GetKycForm'], {"profile_id": profile_id},
                         profile_user_id)['data']['app_kyc_form_by_pk']


def test_send_kyc_form():
    profile_id = PROFILES[0]['id']
    profile_user_id = PROFILES[0]['user_id']

    drivewealth_user_ref_id = None

    for i in range(2):
        response = make_graphql_request(
            QUERIES['SendKycForm'], {"profile_id": profile_id},
            profile_user_id)['data']['send_kyc_form']

        assert response.get("error_message") is None
        assert response.get("status") is not None

        with db_connect() as db_conn:
            with db_conn.cursor() as cursor:
                cursor.execute(
                    "SELECT ref_id from app.drivewealth_users where profile_id = %(profile_id)s",
                    {
                        "profile_id": profile_id,
                    })
                ref_id = cursor.fetchone()[0]

                if drivewealth_user_ref_id is None:
                    drivewealth_user_ref_id = ref_id
                else:
                    assert drivewealth_user_ref_id == ref_id


def test_get_kyc_status():
    profile_id = PROFILES[0]['id']
    profile_user_id = PROFILES[0]['user_id']

    response = make_graphql_request(QUERIES['GetKycStatus'],
                                    {"profile_id": profile_id},
                                    profile_user_id)['data']['get_kyc_status']

    assert response.get("error_message") is None
    assert response.get("status") is not None
