import os
import requests
import json
import datetime
import logging
from common import make_graphql_request, get_personalized_collections, PROFILE_ID, MIN_PERSONALIZED_COLLECTIONS_COUNT

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def test_recommended_collections():
    query = '{ get_recommended_collections(profile_id: %d) { id collection { id name image_url enabled description ticker_collections_aggregate { aggregate { count } } } } }' % (
        PROFILE_ID)
    data = make_graphql_request(query)['data']['get_recommended_collections']

    assert len(data) >= MIN_PERSONALIZED_COLLECTIONS_COUNT

    personalized_collection_ids = set(
        [i['id'] for i in get_personalized_collections()])
    collection_ids = set([i['id'] for i in data])
    assert personalized_collection_ids.issubset(collection_ids)

