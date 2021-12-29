from recommendation.data_access import read_categories_risks, \
    read_profile_category_vector, read_all_ticker_category_and_industry_vectors, read_profile_interest_vectors
from recommendation.match_score.match_score import MatchScore, profile_ticker_similarity

TOP_20_FOR_YOU_COLLECTION_ID = 231


def get_top_by_match_score(db_conn,
                           profile_id: int,
                           k: int = None) -> list[(str, MatchScore)]:
    profile_category_v = read_profile_category_vector(db_conn, profile_id)
    profile_interest_vs = read_profile_interest_vectors(db_conn, profile_id)

    risk_mapping = read_categories_risks(db_conn)

    ticker_vs_list = read_all_ticker_category_and_industry_vectors(db_conn)

    match_score_list = []
    for ticker_vs in ticker_vs_list:
        match_score = profile_ticker_similarity(profile_category_v,
                                                ticker_vs[1], risk_mapping,
                                                profile_interest_vs,
                                                ticker_vs[0])
        match_score_list.append((ticker_vs[0].name, match_score))

    # Uses minus `match_score` to correctly sort the list by both score and symbol
    match_score_list.sort(key=lambda m: (-m[1].match_score(), m[0]))

    return match_score_list[:k] if k else match_score_list
