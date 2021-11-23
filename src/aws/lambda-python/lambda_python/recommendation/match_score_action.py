from typing import List, Dict, Tuple

from common.hasura_function import HasuraAction
from recommendation.core.dim_vector import NamedDimVector
from recommendation.data_access import read_categories_risks, \
    read_profile_industry_vector, \
    read_profile_category_vector, read_ticker_industry_vector, read_ticker_category_vector, \
    read_ticker_industry_vectors_by_collection, read_ticker_category_vectors_by_collection
from recommendation.match_score.match_score import profile_ticker_similarity, is_match


class GetMatchScoreByTicker(HasuraAction):
    def __init__(self):
        super().__init__("get_match_score_by_ticker", "profile_id")

    def apply(self, db_conn, input_params):
        profile_id = input_params["profile_id"]
        profile_category_vector = read_profile_category_vector(db_conn, profile_id)
        profile_industry_vector = read_profile_industry_vector(db_conn, profile_id)

        ticker = input_params["symbol"]
        ticker_category_vector = read_ticker_category_vector(db_conn, ticker)
        ticker_industry_vector = read_ticker_industry_vector(db_conn, ticker)

        risks = read_categories_risks(db_conn)

        match_score = profile_ticker_similarity(profile_category_vector,
                                                ticker_category_vector, risks,
                                                profile_industry_vector,
                                                ticker_industry_vector)
        explanation = match_score.explain()
        return {
            "symbol": ticker,
            "is_match": is_match(profile_category_vector,
                                 ticker_category_vector),
            "match_score": match_score.match_score(),
            "fits_risk": explanation.risk_level.value,
            "fits_categories": explanation.category_level.value,
            "fits_interests": explanation.interest_level.value
        }


#     MATCH SCORE BY COLLECTIONS  #


class GetMatchScoreByCollection(HasuraAction):
    def __init__(self):
        super().__init__("get_match_scores_by_collection", "profile_id")

    def apply(self, db_conn, input_params):
        profile_id = input_params["profile_id"]
        profile_category_vector = read_profile_category_vector(db_conn, profile_id)
        profile_industry_vector = read_profile_industry_vector(db_conn, profile_id)

        risks = read_categories_risks(db_conn)

        collection_id = input_params["collection_id"]
        ticker_industry_vectors = \
            read_ticker_industry_vectors_by_collection(db_conn, profile_id, collection_id)
        ticker_category_vectors = \
            read_ticker_category_vectors_by_collection(db_conn, profile_id, collection_id)

        ticker_category_vectors_dict = self._index_ticker_collection_vectors(
            ticker_category_vectors)
        ticker_industry_vectors_dict = self._index_ticker_collection_vectors(
            ticker_industry_vectors)

        result = []
        all_ticker_collection_pairs = \
            set(ticker_category_vectors_dict.keys()).union(ticker_industry_vectors_dict.keys())
        for symbol in all_ticker_collection_pairs:
            ticker_category_vector = ticker_category_vectors_dict.get(
                symbol, NamedDimVector(symbol, {}))
            ticker_industry_vector = ticker_industry_vectors_dict.get(
                symbol, NamedDimVector(symbol, {}))

            match_score = profile_ticker_similarity(profile_category_vector,
                                                    ticker_category_vector,
                                                    risks,
                                                    profile_industry_vector,
                                                    ticker_industry_vector)

            explanation = match_score.explain()
            result.append({
                "symbol":
                    symbol,
                "is_match":
                    is_match(profile_category_vector, ticker_category_vector),
                "match_score":
                    match_score.match_score(),
                "fits_risk":
                    explanation.risk_level.value,
                "fits_categories":
                    explanation.category_level.value,
                "fits_interests":
                    explanation.interest_level.value
            })

        return result

    @staticmethod
    def _index_ticker_collection_vectors(
            ticker_collection_vectors: List[NamedDimVector]
    ) -> Dict[Tuple[str, int], NamedDimVector]:
        result = {}
        for vector in ticker_collection_vectors:
            result[vector.name] = vector

        return result

