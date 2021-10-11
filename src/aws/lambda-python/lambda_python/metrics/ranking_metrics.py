import getopt
import os
import sys
from typing import Any

import psycopg2
from sklearn.metrics import average_precision_score

from recommendation.collection_ranking import TFIDFWithNorm1_5CollectionRanking, TFCollectionRanking
from recommendation.recommendation_action import get_profile_vector, query_vectors, GetRecommendedCollections

script_dir = os.path.dirname(__file__)

with open(os.path.join(script_dir,
                       "../sql/profile_industries.sql")) as profile_industry_vector_query_file:
    profile_industry_vector_query = profile_industry_vector_query_file.read()

with open(os.path.join(script_dir,
                       "../sql/collection_industries.sql")) as collection_industry_vector_query_file:
    collection_industry_vector_query = collection_industry_vector_query_file.read()


def print_map_metrics(db_conn_string, *rankings):
    with psycopg2.connect(db_conn_string) as db_conn:
        profiles = read_profiles_ids(db_conn)

        fav_cols = read_favourite_collections(db_conn)
        collection_vs = query_vectors(db_conn, collection_industry_vector_query)

        df = GetRecommendedCollections._read_document_frequencies(db_conn)
        corpus_size = GetRecommendedCollections._read_corpus_size(db_conn)
        params = {"df": df, "size": corpus_size}

    for ranking in rankings:
        mean_ap = mean_average_precision(db_conn, profiles, collection_vs, ranking, fav_cols, **params)
        print(f"MAP for {ranking.__class__.__name__}: {mean_ap}")


def read_profiles_ids(db_conn) -> list[int]:
    from operator import itemgetter

    cursor = db_conn.cursor()
    cursor.execute(f"select id from app.profiles;")
    return list(map(itemgetter(0), cursor.fetchall()))


def read_favourite_collections(db_conn) -> dict[Any, set[int]]:
    from itertools import groupby
    from operator import itemgetter

    cursor = db_conn.cursor()
    cursor.execute(f"SELECT profile_id, collection_id FROM app.profile_favorite_collections")

    fav_col_list = list(cursor.fetchall())
    fav_col_list.sort(key=itemgetter(0))

    result = {}
    for (profile_id, collection_ids) in groupby(fav_col_list, key=itemgetter(0)):
        result[profile_id] = set(map(itemgetter(1), collection_ids))

    return result


def mean_average_precision(db_conn, profiles, collection_vs, ranking, fav_cols, **params):
    ap_sum = 0.0
    profile_count = 0
    for profile in profiles:
        profile_v = get_profile_vector(db_conn, profile_industry_vector_query, profile)

        ranked_collections = ranking.rank(profile_v, collection_vs, **params)[:15]

        y_true = [1 if item.item.name in fav_cols.get(profile, []) else 0 for item in ranked_collections]
        y_score = [item.rank_score for item in ranked_collections]

        has_positive_samples = any(filter(lambda v: v == 1, y_true))
        if has_positive_samples:
            profile_ap = average_precision_score(y_true, y_score)

            ap_sum += profile_ap
            profile_count += 1

    mean_ap = ap_sum / profile_count
    return mean_ap


try:
    opts, _ = getopt.getopt(sys.argv[1:], "d:")
except getopt.GetoptError:
    print("ranking_metrics.py -d postgresql://<username>:<password>@<host>:<port>/<database>")
    sys.exit(2)

for opt, arg in opts:
    if opt == "-d":
        print_map_metrics(arg, TFCollectionRanking(), TFIDFWithNorm1_5CollectionRanking())
        sys.exit()
