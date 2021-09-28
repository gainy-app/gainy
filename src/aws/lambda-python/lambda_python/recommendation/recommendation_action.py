import os
from typing import Dict, Tuple, List

from common.hasura_exception import HasuraActionException
from common.hasura_function import HasuraAction
from recommendation.dim_vector import DimVector
from recommendation.match_score import profile_ticker_similarity, is_match

script_dir = os.path.dirname(__file__)

with open(os.path.join(script_dir, "../sql/collection_industries.sql")) as collection_industry_vector_query_file:
    collection_industry_vector_query = collection_industry_vector_query_file.read()

with open(os.path.join(script_dir, "../sql/ticker_categories.sql")) as ticker_category_vector_query_file:
    ticker_category_vector_query = ticker_category_vector_query_file.read()

with open(os.path.join(script_dir, "../sql/ticker_industries.sql")) as ticker_industry_vector_query_file:
    ticker_industry_vector_query = ticker_industry_vector_query_file.read()

with open(os.path.join(script_dir, "../sql/profile_categories.sql")) as profile_category_vector_query_file:
    profile_category_vector_query = profile_category_vector_query_file.read()

with open(os.path.join(script_dir, "../sql/profile_industries.sql")) as profile_industry_vector_query_file:
    profile_industry_vector_query = profile_industry_vector_query_file.read()

with open(os.path.join(script_dir,
                       "../sql/ticker_categories_by_collections.sql")) as ticker_categories_by_collections_query_file:
    ticker_categories_by_collections_query = ticker_categories_by_collections_query_file.read()

with open(os.path.join(script_dir,
                       "../sql/ticker_industries_by_collections.sql")) as ticker_industries_by_collections_query_file:
    ticker_industries_by_collections_query = ticker_industries_by_collections_query_file.read()


#     COMMON UTILS    #

class NamedDimVector(DimVector):

    def __init__(self, name, coordinates):
        super().__init__(coordinates)
        self.name = name


def read_categories_risks(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("SELECT id::varchar, risk_score from public.categories WHERE risk_score IS NOT NULL;")
    return dict(cursor.fetchall())


def get_profile_vector(db_conn, profile_vector_query, profile_id):
    vectors = query_vectors(db_conn, profile_vector_query.format(profile_id))
    if not vectors:
        raise HasuraActionException(400, f"Profile {profile_id} not found")

    return vectors[0]


def get_ticker_vector(db_conn, ticker_vector_query, ticker):
    vectors = query_vectors(db_conn, ticker_vector_query.format(ticker))
    if not vectors:
        raise HasuraActionException(400, f"Symbol {ticker} not found")

    return vectors[0]


def query_vectors(db_conn, query) -> List[NamedDimVector]:
    cursor = db_conn.cursor()
    cursor.execute(query)

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

        sorted_collections = GetRecommendedCollections.query_and_sort_collections(db_conn, profile_id)
        sorted_collection_ids = list(map(lambda c_id: {"id": c_id.name}, sorted_collections))

        return sorted_collection_ids

    @staticmethod
    def query_and_sort_collections(db_conn, profile_id):
        collection_vectors = query_vectors(db_conn, collection_industry_vector_query)
        profile_vector = get_profile_vector(db_conn, profile_industry_vector_query, profile_id)

        # TODO: filter out saved collections

        return GetRecommendedCollections.sort_vectors_by(collection_vectors, profile_vector.cosine_similarity, False)

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
        profile_category_vector = get_profile_vector(db_conn, profile_category_vector_query, profile_id)
        profile_industry_vector = get_profile_vector(db_conn, profile_industry_vector_query, profile_id)

        ticker = input_params["symbol"]
        ticker_category_vector = get_ticker_vector(db_conn, ticker_category_vector_query, ticker)
        ticker_industry_vector = get_ticker_vector(db_conn, ticker_industry_vector_query, ticker)

        risks = read_categories_risks(db_conn)

        match_score = profile_ticker_similarity(
            profile_category_vector,
            ticker_category_vector,
            risks,
            profile_industry_vector,
            ticker_industry_vector
        )

        return {
            "symbol": ticker,
            "is_match": is_match(profile_category_vector, ticker_category_vector),
            "match_score": match_score.match_score(),
            "explanation": [expl.description for expl in match_score.explain(fits_only=True)]
        }


#     MATCH SCORE BY COLLECTIONS  #

class TickerCollectionDimVector(DimVector):

    def __init__(self, symbol, collection_id, coordinates):
        super().__init__(coordinates)
        self.symbol = symbol
        self.collection_id = collection_id


class GetMatchScoreByCollections(HasuraAction):

    def __init__(self):
        super().__init__("get_match_scores_by_collections", "profile_id")

    def apply(self, db_conn, input_params):
        profile_id = input_params["profile_id"]
        profile_category_vector = get_profile_vector(db_conn, profile_category_vector_query, profile_id)
        profile_industry_vector = get_profile_vector(db_conn, profile_industry_vector_query, profile_id)

        risks = read_categories_risks(db_conn)

        collection_ids = input_params["collection_ids"]
        ticker_industry_vectors = \
            self.__get_ticker_vectors_by_collections(db_conn, ticker_categories_by_collections_query, collection_ids)
        ticker_category_vectors = \
            self.__get_ticker_vectors_by_collections(db_conn, ticker_industries_by_collections_query, collection_ids)

        ticker_category_vectors_dict = self.__index_ticker_collection_vectors(ticker_industry_vectors)
        ticker_industry_vectors_dict = self.__index_ticker_collection_vectors(ticker_category_vectors)

        result = []
        all_ticker_collection_pairs = \
            set(ticker_category_vectors_dict.keys()).union(ticker_industry_vectors_dict.keys())
        for (symbol, collection_id) in all_ticker_collection_pairs:
            ticker_category_vector = ticker_category_vectors_dict.get(
                (symbol, collection_id),
                TickerCollectionDimVector(symbol, collection_id, {})
            )
            ticker_industry_vector = ticker_industry_vectors_dict.get(
                (symbol, collection_id),
                TickerCollectionDimVector(symbol, collection_id, {})
            )

            match_score = profile_ticker_similarity(
                profile_category_vector,
                ticker_category_vector,
                risks,
                profile_industry_vector,
                ticker_industry_vector
            )

            result.append(
                {
                    "symbol": symbol,
                    "collection_id": collection_id,
                    "is_match": is_match(profile_category_vector, ticker_category_vector),
                    "match_score": match_score.match_score(),
                    "explanation": [expl.description for expl in match_score.explain(fits_only=True)]
                }
            )

        return result

    @staticmethod
    def __index_ticker_collection_vectors(
            ticker_collection_vectors: List[TickerCollectionDimVector]
    ) -> Dict[Tuple[str, int], TickerCollectionDimVector]:
        result = {}
        for vector in ticker_collection_vectors:
            result[(vector.symbol, vector.collection_id)] = vector

        return result

    @staticmethod
    def __get_ticker_vectors_by_collections(db_conn, ticker_vectors_query, collection_ids):
        compiled_query = ticker_vectors_query.format(", ".join([str(col_id) for col_id in collection_ids]))

        cursor = db_conn.cursor()
        cursor.execute(compiled_query)

        vectors = []
        for row in cursor.fetchall():
            vectors.append(TickerCollectionDimVector(row[0], row[1], row[2]))

        return vectors

    @staticmethod
    def __query_ticker_collection_vectors(db_conn, query) -> List[TickerCollectionDimVector]:
        result = []

        cursor = db_conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            result.append(TickerCollectionDimVector(row[0], row[1], row[2]))

        return result
