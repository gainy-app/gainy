import os
from enum import Enum
from math import sqrt
from typing import Dict, List, Tuple

from common.hasura_exception import HasuraActionException
from recommendation.vectors import DimVector, query_vectors

script_dir = os.path.dirname(__file__)

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


# MATCH SCORE BY COLLECTION

def get_match_score_by_ticker(db_conn, input_params, session_variables):
    profile_id = get_input_param(input_params, "profile_id")
    profile_category_vector = get_profile_vector(db_conn, profile_category_vector_query, profile_id)
    profile_industry_vector = get_profile_vector(db_conn, profile_industry_vector_query, profile_id)

    ticker = get_input_param(input_params, "symbol")
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


# MATCH SCORES BY COLLECTIONS

class TickerCollectionDimVector(DimVector):

    def __init__(self, symbol, collection_id, coordinates):
        super().__init__(coordinates)
        self.symbol = symbol
        self.collection_id = collection_id


def get_match_scores_by_collections(db_conn, input_params, session_variables):
    profile_id = get_input_param(input_params, "profile_id")
    profile_category_vector = get_profile_vector(db_conn, profile_category_vector_query, profile_id)
    profile_industry_vector = get_profile_vector(db_conn, profile_industry_vector_query, profile_id)

    collection_ids = get_input_param(input_params, "collection_ids")
    ticker_industry_vectors = \
        get_ticker_vectors_by_collections(db_conn, ticker_categories_by_collections_query, collection_ids)
    ticker_category_vectors = \
        get_ticker_vectors_by_collections(db_conn, ticker_industries_by_collections_query, collection_ids)

    ticker_category_vectors_dict = index_ticker_collection_vectors(ticker_industry_vectors)
    ticker_industry_vectors_dict = index_ticker_collection_vectors(ticker_category_vectors)

    risks = read_categories_risks(db_conn)

    result = []
    all_ticker_collection_pairs = set(ticker_category_vectors_dict.keys()).union(ticker_industry_vectors_dict.keys())
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


def index_ticker_collection_vectors(
        ticker_collection_vectors: List[TickerCollectionDimVector]
) -> Dict[Tuple[str, int], TickerCollectionDimVector]:
    result = {}
    for vector in ticker_collection_vectors:
        result[(vector.symbol, vector.collection_id)] = vector

    return result


def get_ticker_vectors_by_collections(db_conn, ticker_vectors_query, collection_ids):
    compiled_query = ticker_vectors_query.format(", ".join([str(col_id) for col_id in collection_ids]))

    cursor = db_conn.cursor()
    cursor.execute(compiled_query)

    vectors = []
    for row in cursor.fetchall():
        vectors.append(TickerCollectionDimVector(row[0], row[1], row[2]))

    return vectors


def query_ticker_collection_vectors(db_conn, query) -> List[TickerCollectionDimVector]:
    result = []

    cursor = db_conn.cursor()
    cursor.execute(query)
    for row in cursor.fetchall():
        result.append(TickerCollectionDimVector(row[0], row[1], row[2]))

    return result


def read_categories_risks(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("SELECT id::varchar, risk_score from public.categories WHERE risk_score IS NOT NULL;")
    return dict(cursor.fetchall())


def get_ticker_vector(db_conn, ticker_vector_query, ticker):
    vectors = query_vectors(db_conn, ticker_vector_query.format(ticker))
    if not vectors:
        raise HasuraActionException(400, f"Symbol {ticker} not found")

    return vectors[0]


def get_profile_vector(db_conn, profile_vector_query, profile_id):
    vectors = query_vectors(db_conn, profile_vector_query.format(profile_id))
    if not vectors:
        raise HasuraActionException(400, f"Profile {profile_id} not found")

    return vectors[0]


def get_input_param(input_params, param_name):
    profile_id = input_params.get(param_name, None)
    if not profile_id:
        raise HasuraActionException(400, f"{param_name} is not provided")

    return profile_id


# IS MATCH

def is_match(profile_category_vector: DimVector, ticker_category_vector: DimVector) -> bool:
    profile_categories = profile_category_vector.coordinates.keys()
    ticker_categories = ticker_category_vector.coordinates.keys()

    return len(set(profile_categories).intersection(ticker_categories)) > 0


# INDUSTRY SIMILARITY SCORE

def normalize_profile_industries_vector(vector: DimVector) -> DimVector:
    max_value = max(vector.coordinates.values())
    min_value = min(vector.coordinates.values())

    new_coordinates = {}
    denominator = 1.0 + sqrt(max_value) - sqrt(min_value)
    for dimension in vector.coordinates.keys():
        new_coordinates[dimension] = (1.0 + sqrt(vector.coordinates[dimension]) - sqrt(min_value)) / denominator

    return DimVector(new_coordinates)


def get_industry_similarity(
        profile_industries: DimVector,
        ticker_industries: DimVector
) -> float:
    return DimVector.dot_product(
        normalize_profile_industries_vector(profile_industries),
        ticker_industries
    )


# RISK SIMILARITY SCORE

RISK_TO_SCORE_MAPPING = {
    1: 0.0,
    2: 0.5,
    3: 1.0
}


def get_categories_risk_score(categories: DimVector, risk_mapping: Dict[str, int]):
    risk_sum = None
    categories_num = None
    for category in categories.coordinates.keys():
        risk = risk_mapping.get(category, None)
        if risk:
            if not risk_sum:
                risk_sum = categories.coordinates[category] * RISK_TO_SCORE_MAPPING[risk]
                categories_num = categories.coordinates[category]
            else:
                risk_sum += categories.coordinates[category] * RISK_TO_SCORE_MAPPING[risk]
                categories_num += categories.coordinates[category]

    if risk_sum is None or categories_num is None:
        return None
    else:
        return risk_sum / categories_num


def get_risk_similarity(
        profile_categories: DimVector,
        ticker_categories: DimVector,
        risk_mapping: Dict[str, int]
) -> float:
    profile_risk_score = get_categories_risk_score(profile_categories, risk_mapping)
    ticker_risk_score = get_categories_risk_score(ticker_categories, risk_mapping)

    if profile_risk_score is None or ticker_risk_score is None:
        return 0.0

    return 1.0 - sqrt((profile_risk_score - ticker_risk_score) * (profile_risk_score - ticker_risk_score))


# CATEGORY SIMILARITY SCORE

def get_category_similarity(
        profile_categories: DimVector,
        ticker_categories: DimVector
) -> float:
    return profile_categories.cosine_similarity(ticker_categories)


# GENERAL SIMILARITY SCORE

class MatchScoreComponent(Enum):
    RISK = "risk"
    CATEGORY = "category"
    INDUSTRY = "industry"


class MatchScoreExplanation:

    def __init__(
            self,
            component: MatchScoreComponent,
            fits: bool,
            description: str
    ):
        self.component = component
        self.fits = fits
        self.description = description


EXPLANATION_CONFIG = {
    MatchScoreComponent.RISK: [
        (0.0, 0.3, False, "Doesn't fit your risk profile"),
        (0.7, 1.0, True, "Fits your risk profile")
    ],
    MatchScoreComponent.CATEGORY: [
        (0.0, 0.3, False, "Doesn't fit your preferred categories"),
        (0.7, 1.0, True, "Fits your preferred categories")
    ],
    MatchScoreComponent.INDUSTRY: [
        (0.0, 0.3, False, "Doesn't fit your interests"),
        (0.7, 1.0, True, "Fits your interests")
    ]
}


class MatchScoreExplainer:

    def __init__(self, config):
        self.config = config

    def __apply_explanation_config(self, similarity, component) -> List[MatchScoreExplanation]:
        explanations = []
        for lower_bound, upper_bound, fits, description in self.config[component]:
            if lower_bound <= similarity <= upper_bound:
                explanations.append(MatchScoreExplanation(component, fits, description))

        return explanations

    def explain(self, risk_similarity, category_similarity, industry_similarity) -> List[MatchScoreExplanation]:
        explanations = []

        explanations += self.__apply_explanation_config(risk_similarity, MatchScoreComponent.RISK)
        explanations += self.__apply_explanation_config(category_similarity, MatchScoreComponent.CATEGORY)
        explanations += self.__apply_explanation_config(industry_similarity, MatchScoreComponent.INDUSTRY)

        return explanations


class MatchScore:
    def __init__(self,
                 similarity: float,
                 risk_similarity: float,
                 category_similarity: float,
                 industry_similarity: float
                 ):
        self.similarity = similarity

        self.risk_similarity = risk_similarity
        self.category_similarity = category_similarity
        self.industry_similarity = industry_similarity

        self.similarity_explainer = MatchScoreExplainer(EXPLANATION_CONFIG)

    def explain(self, fits_only: bool = False) -> List[MatchScoreExplanation]:
        explanations = self.similarity_explainer.explain(
            self.risk_similarity,
            self.category_similarity,
            self.industry_similarity
        )

        if fits_only:
            return list(filter(lambda expl: expl.fits, explanations))
        else:
            return explanations

    def match_score(self):
        return round(self.similarity * 100)


def profile_ticker_similarity(
        profile_categories: DimVector,
        ticker_categories: DimVector,
        risk_mapping: Dict[str, int],
        profile_industries: DimVector,
        ticker_industries: DimVector,
) -> MatchScore:
    risk_weight = 1 / 3
    category_weight = 1 / 3
    industry_weight = 1 / 3

    risk_similarity = get_risk_similarity(profile_categories, ticker_categories, risk_mapping)
    category_similarity = get_category_similarity(profile_categories, ticker_categories)
    industry_similarity = get_industry_similarity(profile_industries, ticker_industries)

    similarity = risk_weight * risk_similarity + category_weight * category_similarity + industry_weight * industry_similarity

    return MatchScore(similarity, risk_similarity, category_similarity, industry_similarity)
