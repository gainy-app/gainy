import getopt
import sys
from typing import Any

import psycopg2
from sklearn.metrics import average_precision_score

from recommendation.collection_ranking import TFIDFWithNorm1_5CollectionRanking, TFCollectionRanking, \
    MatchScoreCollectionRanking
from recommendation.data_access import read_profile_industry_vector, read_all_collection_industry_vectors, \
    read_industry_frequencies, read_industry_corpus_size, read_all_ticker_category_and_industry_vectors, \
    read_profile_category_vector, read_profile_interest_vectors, read_categories_risks, read_collection_tickers
from recommendation.match_score.match_score import profile_ticker_similarity


def print_map_metrics(db_conn_string, *rankings):
    with psycopg2.connect(db_conn_string) as db_conn:
        profiles = read_profiles_ids(db_conn)

        fav_cols = read_favourite_collections(db_conn)
        collection_vs = read_all_collection_industry_vectors(db_conn)

        df = read_industry_frequencies(db_conn)
        corpus_size = read_industry_corpus_size(db_conn)

        params = {"df": df, "size": corpus_size}

    for ranking in rankings:
        mean_ap = mean_average_precision(db_conn, profiles, collection_vs,
                                         ranking, fav_cols, **params)
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
    cursor.execute(
        f"SELECT profile_id, collection_id FROM app.profile_favorite_collections"
    )

    fav_col_list = list(cursor.fetchall())
    fav_col_list.sort(key=itemgetter(0))

    result = {}
    for (profile_id, collection_ids) in groupby(fav_col_list,
                                                key=itemgetter(0)):
        result[profile_id] = set(map(itemgetter(1), collection_ids))

    return result


def mean_average_precision(db_conn, profiles, collection_vs, ranking, fav_cols,
                           **params):
    ap_sum = 0.0
    profile_count = 0
    for profile in profiles:
        profile_v = read_profile_industry_vector(db_conn, profile)

        ticker_vs_list = read_all_ticker_category_and_industry_vectors(db_conn)
        profile_category_v = read_profile_category_vector(db_conn, profile)
        profile_interest_vs = read_profile_interest_vectors(db_conn, profile)

        risk_mapping = read_categories_risks(db_conn)

        ticker_match_scores = {}
        for ticker_vs in ticker_vs_list:
            match_score = profile_ticker_similarity(profile_category_v,
                                                    ticker_vs[1],
                                                    risk_mapping,
                                                    profile_interest_vs,
                                                    ticker_vs[0])
            ticker_match_scores[ticker_vs[0].name] = match_score.match_score()

        collection_tickers = {}
        for collection_v in collection_vs:
            collection_tickers[collection_v.name] = read_collection_tickers(db_conn, profile, collection_v.name)

        params["collection_tickers"] = collection_tickers
        params["ticker_match_scores"] = ticker_match_scores

        ranked_collections = ranking.rank(profile_v, collection_vs,
                                          **params)[:15]

        y_true = [
            1 if item.item.name in fav_cols.get(profile, []) else 0
            for item in ranked_collections
        ]
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
    print(
        "ranking_metrics.py -d postgresql://<username>:<password>@<host>:<port>/<database>"
    )
    sys.exit(2)

for opt, arg in opts:
    if opt == "-d":
        print_map_metrics(arg, TFCollectionRanking(),
                          TFIDFWithNorm1_5CollectionRanking(), MatchScoreCollectionRanking())
        sys.exit()
