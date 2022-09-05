import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from common import make_graphql_request, db_connect
from trading.common import fill_kyc_form, kyc_send_form, load_query, PROFILES


def test_upsert_kyc_form():
    profile_id = PROFILES[0]['id']
    profile_user_id = PROFILES[0]['user_id']

    fill_kyc_form(profile_id, profile_user_id)


def test_get_kyc_form():
    profile_id = PROFILES[0]['id']
    profile_user_id = PROFILES[0]['user_id']

    make_graphql_request(load_query('kyc',
                                    'GetKycForm'), {"profile_id": profile_id},
                         profile_user_id)['data']['app_kyc_form_by_pk']


def test_kyc_send_form():
    profile_id = PROFILES[0]['id']
    profile_user_id = PROFILES[0]['user_id']

    drivewealth_user_ref_id = None

    for i in range(2):
        kyc_send_form(profile_id, profile_user_id)

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


def test_kyc_get_status():
    profile_id = PROFILES[0]['id']
    profile_user_id = PROFILES[0]['user_id']

    response = make_graphql_request(load_query('kyc', 'KycGetStatus'),
                                    {"profile_id": profile_id},
                                    profile_user_id)['data']['kyc_get_status']

    assert response.get("error_message") is None
    assert response.get("status") is not None
