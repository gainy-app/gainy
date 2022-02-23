import os
from common import make_graphql_request, get_personalized_collections, PROFILE_ID, MIN_PERSONALIZED_COLLECTIONS_COUNT


def get_recommended_collections():
    query = '{ get_recommended_collections(profile_id: %d) { id collection { id name image_url enabled description ticker_collections_aggregate { aggregate { count } } } } }' % (
        PROFILE_ID)
    return make_graphql_request(query)['data']['get_recommended_collections']


def test_recommended_collections():
    data = get_recommended_collections()

    assert len(data) >= MIN_PERSONALIZED_COLLECTIONS_COUNT

    personalized_collection_ids = set(
        [i['id'] for i in get_personalized_collections()])
    collection_ids = set([i['id'] for i in data])
    assert personalized_collection_ids.issubset(collection_ids)


def test_favorite_collections():
    data = get_recommended_collections()
    collection_id = data[0]['id']

    query = 'mutation InsertProfileFavoriteCollection($profileID: Int!, $collectionID: Int!){ insert_app_profile_favorite_collections(objects: {collection_id: $collectionID, profile_id: $profileID}, on_conflict: { constraint: profile_favorite_collections_pkey, update_columns: []}) { returning { collection_id } } }'
    make_graphql_request(query, {
        "profileID": PROFILE_ID,
        "collectionID": collection_id
    })

    query = 'mutation DeleteProfileFavoriteCollection($profileID: Int!, $collectionID: Int!){ delete_app_profile_favorite_collections( where: { collection_id: {_eq: $collectionID}, profile_id: {_eq: $profileID} } ) { returning { collection_id } } }'
    make_graphql_request(query, {
        "profileID": PROFILE_ID,
        "collectionID": collection_id
    })


def test_collection_metrics():
    collection_ids = [83]

    for collection_id in collection_ids:
        query = 'mutation InsertProfileFavoriteCollection($profileID: Int!, $collectionID: Int!){ insert_app_profile_favorite_collections(objects: {collection_id: $collectionID, profile_id: $profileID}, on_conflict: { constraint: profile_favorite_collections_pkey, update_columns: []}) { returning { collection_id } } }'
        make_graphql_request(query, {
            "profileID": PROFILE_ID,
            "collectionID": collection_id
        })

    query_file = os.path.join(os.path.dirname(__file__),
                              'queries/GetHomeTabData.graphql')
    with open(query_file, 'r') as f:
        query = f.read()
    data = make_graphql_request(query, {
        "profileId": PROFILE_ID,
        "rankedCount": 100
    })['data']
    assert len(data['profile_collection_tickers_performance_ranked']) >= 1
    assert len(data['app_profile_favorite_collections']) >= len(collection_ids)
    for i in data['app_profile_favorite_collections']:
        assert i['collection']['metrics']['relative_daily_change'] is not None

    for collection_id in collection_ids:
        query = 'mutation DeleteProfileFavoriteCollection($profileID: Int!, $collectionID: Int!){ delete_app_profile_favorite_collections( where: { collection_id: {_eq: $collectionID}, profile_id: {_eq: $profileID} } ) { returning { collection_id } } }'
        make_graphql_request(query, {
            "profileID": PROFILE_ID,
            "collectionID": collection_id
        })
