from abc import ABC
from typing import List, Dict, Tuple

from common.hasura_function import HasuraAction
from recommendation.core.dim_vector import NamedDimVector
from recommendation.data_access import read_categories_risks, read_profile_industry_vector, \
    read_profile_category_vector, read_ticker_industry_vectors, read_ticker_category_vectors, read_collection_tickers, \
    read_profile_interest_vectors
from recommendation.match_score.match_score import profile_ticker_similarity


class AbstractMatchScoreAction(HasuraAction, ABC):
    def __init__(self, name, profile_id_param):
        super().__init__(name, profile_id_param)

    def compute_match_scores(self, db_conn, profile_id, symbols):
        profile_category_vector = read_profile_category_vector(
            db_conn, profile_id)
        profile_interest_vectors = read_profile_interest_vectors(
            db_conn, profile_id)

        risks = read_categories_risks(db_conn)

        ticker_industry_vectors = read_ticker_industry_vectors(
            db_conn, symbols)
        ticker_category_vectors = read_ticker_category_vectors(
            db_conn, symbols)

        ticker_category_vectors_dict = self._index_ticker_collection_vectors(
            ticker_category_vectors)
        ticker_industry_vectors_dict = self._index_ticker_collection_vectors(
            ticker_industry_vectors)

        result = []
        for symbol in symbols:
            ticker_category_vector = ticker_category_vectors_dict.get(
                symbol, NamedDimVector(symbol, {}))
            ticker_industry_vector = ticker_industry_vectors_dict.get(
                symbol, NamedDimVector(symbol, {}))

            match_score = profile_ticker_similarity(profile_category_vector,
                                                    ticker_category_vector,
                                                    risks,
                                                    profile_interest_vectors,
                                                    ticker_industry_vector)

            explanation = match_score.explain()
            result.append({
                "symbol":
                symbol,
                "match_score":
                match_score.match_score(),
                "fits_risk":
                explanation.risk_level.value,
                "risk_similarity":
                explanation.risk_similarity,
                "fits_categories":
                explanation.category_level.value,
                "category_matches":
                explanation.category_matches,
                "fits_interests":
                explanation.interest_level.value,
                "interest_matches":
                explanation.interest_matches
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


class GetMatchScoreByTicker(AbstractMatchScoreAction):
    def __init__(self):
        super().__init__("get_match_score_by_ticker", "profile_id")

    def apply(self, db_conn, input_params, headers):
        profile_id = input_params["profile_id"]
        ticker = input_params["symbol"]

        return super().compute_match_scores(db_conn, profile_id, [ticker])[0]


class GetMatchScoreByTickerList(AbstractMatchScoreAction):
    def __init__(self):
        super().__init__("get_match_scores_by_ticker_list", "profile_id")

    def apply(self, db_conn, input_params, headers):
        profile_id = input_params["profile_id"]
        tickers = input_params["symbols"]

        return super().compute_match_scores(db_conn, profile_id, tickers)


class GetMatchScoreByCollection(AbstractMatchScoreAction):
    def __init__(self):
        super().__init__("get_match_scores_by_collection", "profile_id")

    def apply(self, db_conn, input_params, headers):
        profile_id = input_params["profile_id"]
        collection_id = input_params["collection_id"]

        collection_tickers = read_collection_tickers(db_conn, profile_id,
                                                     collection_id)

        return super().compute_match_scores(db_conn, profile_id,
                                            collection_tickers)
