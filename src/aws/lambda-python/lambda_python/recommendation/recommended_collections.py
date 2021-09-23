import os

from common.hasura_exception import HasuraActionException
from recommendation.vectors import sort_vectors_by, query_vectors

script_dir = os.path.dirname(__file__)

with open(os.path.join(script_dir, "../sql/profile_industries.sql")) as profile_industry_vector_query_file:
    profile_score_query = profile_industry_vector_query_file.read()

with open(os.path.join(script_dir, "../sql/collection_industries.sql")) as collection_industry_vector_query_file:
    collection_score_query = collection_industry_vector_query_file.read()


def get_recommended_collections(db_conn, input_params, session_variables):
    profile_id = input_params.get("profile_id", None)
    if not profile_id:
        raise HasuraActionException(400, "Profile id is not provided")

    sorted_collections = query_and_sort_collections(db_conn, profile_id)
    sorted_collection_ids = list(map(lambda c_id: {"id": c_id.name}, sorted_collections))

    return sorted_collection_ids


def query_and_sort_collections(db_conn, profile_id):
    collections = query_vectors(db_conn, collection_score_query)
    profile_list = query_vectors(db_conn, profile_score_query.format(profile_id))
    if len(profile_list) == 0:
        raise HasuraActionException(400, f"Incorrect profile id: {profile_id}")

    profile = profile_list[0]
    return sort_vectors_by(collections, profile.cosine_similarity, False)

