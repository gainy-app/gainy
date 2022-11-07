import time
from hasura_tests.common import make_graphql_request

PROFILES = make_graphql_request("{app_profiles{id, user_id}}",
                                user_id=None)['data']['app_profiles']


def test_invitations():
    from_profile_id = PROFILES[0]['id']
    from_profile_user_id = PROFILES[0]['user_id']
    to_profile_id = PROFILES[1]['id']
    to_profile_user_id = PROFILES[1]['user_id']

    profile_data = make_graphql_request(
        "{app_profiles{id, subscription_end_date}}", {},
        from_profile_user_id)['data']['app_profiles'][0]
    assert profile_data['id'] == from_profile_id
    assert profile_data['subscription_end_date'] is None

    query = "mutation CreateInvitation($fromProfileId: Int!, $toProfileId: Int!) { insert_app_invitations_one(object: {from_profile_id: $fromProfileId, to_profile_id: $toProfileId}) { id } }"
    data = make_graphql_request(query, {
        "fromProfileId": from_profile_id,
        "toProfileId": to_profile_id
    }, to_profile_user_id)['data']['insert_app_invitations_one']
    assert data['id'] is not None

    time.sleep(1.5)

    profile_data = make_graphql_request(
        "{app_profiles{id, subscription_end_date}}", {},
        from_profile_user_id)['data']['app_profiles'][0]
    assert profile_data['id'] == from_profile_id
    assert profile_data['subscription_end_date'] is not None

    profile_data = make_graphql_request(
        "{app_profiles{id, subscription_end_date}}", {},
        to_profile_user_id)['data']['app_profiles'][0]
    assert profile_data['id'] == to_profile_id
    assert profile_data['subscription_end_date'] is not None


def test_promocodes():
    query = "query GetPromocode($code: String!) { get_promocode(code: $code) { id name description config } }"
    data = make_graphql_request(query, {
        "code": "EEKUA1AE",
    })['data']['get_promocode']
    assert data is not None
    assert data['id'] is not None

    query = "query GetPromocode($code: String!) { get_promocode(code: $code) { id name description config } }"
    data = make_graphql_request(query, {
        "code": "foo",
    })['data']['get_promocode']
    assert data is None
