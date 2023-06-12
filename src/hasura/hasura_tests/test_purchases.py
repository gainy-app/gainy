from hasura_tests.common import make_graphql_request

PROFILES = make_graphql_request("{app_profiles{id, user_id}}",
                                user_id=None)['data']['app_profiles']


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
