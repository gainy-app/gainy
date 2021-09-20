import os

from common.hasura_exception import HasuraActionException
from recommendation.vectors import SparseVector, sort_vectors_by

script_dir = os.path.dirname(__file__)

with open(os.path.join(script_dir, "../data/sql/profile_score_query.sql")) as profile_score_query_file:
    profile_score_query = profile_score_query_file.read()

with open(os.path.join(script_dir, "../data/sql/collection_score_query.sql")) as profile_score_query_file:
    collection_score_query = profile_score_query_file.read()


def recommend_profile_collections(db_conn, input_params, session_variables):
    profile_id = input_params.get("profile_id", None)
    if not profile_id:
        raise HasuraActionException(400, "Profile id is not provided")

    sorted_collections = query_and_sorted_collections(db_conn, profile_id)
    sorted_collection_ids = list(map(lambda c_id: {"id": c_id.id}, sorted_collections))

    return sorted_collection_ids


def query_and_sorted_collections(db_conn, profile_id):
    collections = query_vectors(db_conn, collection_score_query)
    profile_list = query_vectors(db_conn, profile_score_query.format(profile_id))
    if len(profile_list) == 0:
        raise HasuraActionException(400, f"Incorrect profile id: {profile_id}")

    profile = profile_list[0]
    return sort_vectors_by(collections, profile.cosine_similarity, False)


def query_vectors(db_conn, query):
    result = []

    cursor = db_conn.cursor()
    cursor.execute(query)
    for row in cursor.fetchall():
        result.append(SparseVector(row[0], row[1]))

    return result
