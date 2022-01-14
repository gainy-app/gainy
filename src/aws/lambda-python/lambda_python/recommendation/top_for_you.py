import time

from recommendation.data_access import read_categories_risks, \
    read_profile_category_vector, read_all_ticker_category_and_industry_vectors, read_profile_interest_vectors
from recommendation.match_score.match_score import MatchScore, profile_ticker_similarity
from utils.performance import metric_value

TOP_20_FOR_YOU_COLLECTION_ID = 231


def get_top_by_match_score(db_conn,
                           profile_id: int,
                           k: int = None) -> list[(str, MatchScore)]:

    start = time.time()
    profile_category_v = read_profile_category_vector(db_conn, profile_id)
    profile_interest_vs = read_profile_interest_vectors(db_conn, profile_id)
    print(f"LAMBDA_PROFILE: Load profile data: {time.time() - start}")

    start = time.time()
    risk_mapping = read_categories_risks(db_conn)
    print(f"LAMBDA_PROFILE: Load category risk data: {time.time() - start}")

    start = time.time()
    ticker_vs_list = read_all_ticker_category_and_industry_vectors(db_conn)
    print(f"LAMBDA_PROFILE: Load all ticker data: {time.time() - start}")

    start = time.time()
    match_score_list = []
    for ticker_vs in ticker_vs_list:
        match_score = profile_ticker_similarity(profile_category_v,
                                                ticker_vs[1], risk_mapping,
                                                profile_interest_vs,
                                                ticker_vs[0])
        match_score_list.append((ticker_vs[0].name, match_score))

    print(f"LAMBDA_PROFILE: Metric `get_risk_similarity`: {metric_value('get_risk_similarity')}")
    print(f"LAMBDA_PROFILE: Metric `get_category_similarity`: {metric_value('get_category_similarity')}")
    print(f"LAMBDA_PROFILE: Metric `get_interest_similarity`: {metric_value('get_interest_similarity')}")
    print(f"LAMBDA_PROFILE: Metric `get_interest_similarity.interest_score`: {metric_value('get_interest_similarity.interest_score')}")

    print(f"LAMBDA_PROFILE: Metric `get_interest_similarity.interest_score.counter`: {metric_value('get_interest_similarity.interest_score.counter')}")
    print(f"LAMBDA_PROFILE: Metric `get_interest_similarity.interest_score.counter.flatten_list`: {metric_value('get_interest_similarity.interest_score.counter.flatten_list')}")
    print(f"LAMBDA_PROFILE: Metric `get_interest_similarity.interest_score.counter.counter`: {metric_value('get_interest_similarity.interest_score.counter.counter')}")
    print(f"LAMBDA_PROFILE: Metric `get_interest_similarity.interest_score.counter.dim_vector`: {metric_value('get_interest_similarity.interest_score.counter.dim_vector')}")

    print(f"LAMBDA_PROFILE: Metric `get_interest_similarity.interest_score.dot_product`: {metric_value('get_interest_similarity.interest_score.dot_product')}")
    print(f"LAMBDA_PROFILE: Metric `get_interest_similarity.interest_score.dot_product.norm_profile`: {metric_value('get_interest_similarity.interest_score.dot_product.norm_profile')}")
    print(f"LAMBDA_PROFILE: Metric `normalized_profile_industries_vector.min_max`: {metric_value('normalized_profile_industries_vector.min_max')}")
    print(f"LAMBDA_PROFILE: Metric `normalized_profile_industries_vector.new_coordinates`: {metric_value('normalized_profile_industries_vector.new_coordinates')}")
    print(f"LAMBDA_PROFILE: Metric `normalized_profile_industries_vector.zip`: {metric_value('normalized_profile_industries_vector.zip')}")
    print(f"LAMBDA_PROFILE: Metric `normalized_profile_industries_vector.dim_vector`: {metric_value('normalized_profile_industries_vector.dim_vector')}")
    print(f"LAMBDA_PROFILE: Metric `get_interest_similarity.interest_score.dot_product.norm_ticker`: {metric_value('get_interest_similarity.interest_score.dot_product.norm_ticker')}")
    print(f"LAMBDA_PROFILE: Metric `get_interest_similarity.interest_score.dot_product.dot_product`: {metric_value('get_interest_similarity.interest_score.dot_product.dot_product')}")

    print(f"LAMBDA_PROFILE: Metric `get_interest_similarity.interest_matches`: {metric_value('get_interest_similarity.interest_matches')}")
    print(f"LAMBDA_PROFILE: Metric `get_interest_similarity.interest_matches.dot_product`: {metric_value('get_interest_similarity.interest_matches.dot_product')}")

    print(f"LAMBDA_PROFILE: Compute match scores: {time.time() - start}")

    start = time.time()
    # Uses minus `match_score` to correctly sort the list by both score and symbol
    match_score_list.sort(key=lambda m: (-m[1].match_score(), m[0]))

    print(f"LAMBDA_PROFILE: Sort match scores: {time.time() - start}")

    return match_score_list[:k] if k else match_score_list
