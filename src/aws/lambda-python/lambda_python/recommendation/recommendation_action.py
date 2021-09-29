import os
from typing import Dict, Tuple, List

from common.hasura_exception import HasuraActionException
from common.hasura_function import HasuraAction
from recommendation.dim_vector import DimVector
from recommendation.match_score import profile_ticker_similarity, is_match

script_dir = os.path.dirname(__file__)

with open(os.path.join(script_dir, "../sql/collection_industries.sql")
          ) as collection_industry_vector_query_file:
    collection_industry_vector_query = collection_industry_vector_query_file.read(
    )

with open(os.path.join(
        script_dir,
        "../sql/ticker_categories.sql")) as ticker_category_vector_query_file:
    ticker_category_vector_query = ticker_category_vector_query_file.read()

with open(os.path.join(
        script_dir,
        "../sql/ticker_industries.sql")) as ticker_industry_vector_query_file:
    ticker_industry_vector_query = ticker_industry_vector_query_file.read()

with open(os.path.join(script_dir, "../sql/profile_categories.sql")
          ) as profile_category_vector_query_file:
    profile_category_vector_query = profile_category_vector_query_file.read()

with open(os.path.join(script_dir, "../sql/profile_industries.sql")
          ) as profile_industry_vector_query_file:
    profile_industry_vector_query = profile_industry_vector_query_file.read()

with open(
        os.path.join(script_dir, "../sql/ticker_categories_by_collection.sql")
) as ticker_categories_by_collection_query_file:
    ticker_categories_by_collection_query = ticker_categories_by_collection_query_file.read(
    )

with open(
        os.path.join(script_dir, "../sql/ticker_industries_by_collection.sql")
) as ticker_industries_by_collection_query_file:
    ticker_industries_by_collection_query = ticker_industries_by_collection_query_file.read(
    )

#     COMMON UTILS    #


class NamedDimVector(DimVector):
    def __init__(self, name, coordinates):
        super().__init__(coordinates)
        self.name = name


def read_categories_risks(db_conn):
    cursor = db_conn.cursor()
    cursor.execute(
        "SELECT id::varchar, risk_score from public.categories WHERE risk_score IS NOT NULL;"
    )
    return dict(cursor.fetchall())


def get_profile_vector(db_conn, profile_vector_query, profile_id):
    vectors = query_vectors(db_conn, profile_vector_query,
                            {"profile_id": profile_id})
    if not vectors:
        raise HasuraActionException(400, f"Profile {profile_id} not found")

    return vectors[0]


def get_ticker_vector(db_conn, ticker_vector_query, ticker):
    vectors = query_vectors(db_conn, ticker_vector_query, {"symbol": ticker})
    if not vectors:
        raise HasuraActionException(400, f"Symbol {ticker} not found")

    return vectors[0]


def query_vectors(db_conn, query, variables=None) -> List[NamedDimVector]:
    cursor = db_conn.cursor()
    cursor.execute(query, variables)

    vectors = []
    for row in cursor.fetchall():
        vectors.append(NamedDimVector(row[0], row[1]))

    return vectors


#     RECOMMEND COLLECTIONS    #


class GetRecommendedCollections(HasuraAction):
    def __init__(self):
        super().__init__("get_recommended_collections", "profile_id")

    def apply(self, db_conn, input_params):
        profile_id = input_params["profile_id"]

        sorted_collections = GetRecommendedCollections.query_and_sort_collections(
            db_conn, profile_id)
        sorted_collection_ids = list(
            map(lambda c_id: {"id": c_id.name}, sorted_collections))

        return sorted_collection_ids

    @staticmethod
    def query_and_sort_collections(db_conn, profile_id):
        collection_vectors = query_vectors(db_conn,
                                           collection_industry_vector_query)
        profile_vector = get_profile_vector(db_conn,
                                            profile_industry_vector_query,
                                            profile_id)

        return GetRecommendedCollections.sort_vectors_by(
            collection_vectors, profile_vector.cosine_similarity, False)

    @staticmethod
    def sort_vectors_by(vectors, similarity, asc=True):
        vectors.sort(key=similarity, reverse=not asc)
        return vectors


#     MATCH SCORE BY TICKER  #


class GetMatchScoreByTicker(HasuraAction):
    def __init__(self):
        super().__init__("get_match_score_by_ticker", "profile_id")

    def apply(self, db_conn, input_params):
        profile_id = input_params["profile_id"]
        profile_category_vector = get_profile_vector(
            db_conn, profile_category_vector_query, profile_id)
        profile_industry_vector = get_profile_vector(
            db_conn, profile_industry_vector_query, profile_id)

        ticker = input_params["symbol"]
        ticker_category_vector = get_ticker_vector(
            db_conn, ticker_category_vector_query, ticker)
        ticker_industry_vector = get_ticker_vector(
            db_conn, ticker_industry_vector_query, ticker)

        risks = read_categories_risks(db_conn)

        match_score = profile_ticker_similarity(profile_category_vector,
                                                ticker_category_vector, risks,
                                                profile_industry_vector,
                                                ticker_industry_vector)
        explanation = match_score.explain()
        return {
            "symbol":
            ticker,
            "is_match":
            is_match(profile_category_vector, ticker_category_vector),
            "match_score":
            match_score.match_score(),
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
        profile_category_vector = get_profile_vector(
            db_conn, profile_category_vector_query, profile_id)
        profile_industry_vector = get_profile_vector(
            db_conn, profile_industry_vector_query, profile_id)

        risks = read_categories_risks(db_conn)

        collection_id = input_params["collection_id"]
        ticker_industry_vectors = \
            self._get_ticker_vectors_by_collection(db_conn, ticker_categories_by_collection_query, collection_id)
        ticker_category_vectors = \
            self._get_ticker_vectors_by_collection(db_conn, ticker_industries_by_collection_query, collection_id)

        ticker_category_vectors_dict = self._index_ticker_collection_vectors(
            ticker_industry_vectors)
        ticker_industry_vectors_dict = self._index_ticker_collection_vectors(
            ticker_category_vectors)

        result = []
        all_ticker_collection_pairs = \
            set(ticker_category_vectors_dict.keys()).union(ticker_industry_vectors_dict.keys())
        for symbol in all_ticker_collection_pairs:
            ticker_category_vector = ticker_category_vectors_dict.get(
                symbol,
                NamedDimVector(symbol, {}))
            ticker_industry_vector = ticker_industry_vectors_dict.get(
                symbol,
                NamedDimVector(symbol, {}))

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
                "fits_risk": explanation.risk_level.value,
                "fits_categories": explanation.category_level.value,
                "fits_interests": explanation.interest_level.value
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

    @staticmethod
    def _get_ticker_vectors_by_collection(db_conn, ticker_vectors_query,
                                          collection_id):
        cursor = db_conn.cursor()
        cursor.execute(ticker_vectors_query,
                       {"collection_id": collection_id})

        vectors = []
        for row in cursor.fetchall():
            vectors.append(NamedDimVector(row[0], row[1]))

        return vectors

    @staticmethod
    def _query_ticker_collection_vectors(
            db_conn, query) -> List[NamedDimVector]:
        result = []

        cursor = db_conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            result.append(NamedDimVector(row[0], row[1]))

        return result
