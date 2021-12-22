from enum import Enum
from math import sqrt
from typing import Dict, List

from recommendation.core.dim_vector import DimVector, NamedDimVector


# IS MATCH

# TODO: deprecated

def is_match(profile_category_vector: DimVector,
             ticker_category_vector: DimVector) -> bool:
    profile_categories = profile_category_vector.dims
    ticker_categories = ticker_category_vector.dims

    return len(set(profile_categories).intersection(ticker_categories)) > 0


# INDUSTRY SIMILARITY SCORE


def get_interest_similarity(profile_interest_vs: List[NamedDimVector],
                            ticker_industry_v: DimVector) -> (float, List[str]):
    interest_score = 0
    interest_matches = []

    for profile_interest_v in profile_interest_vs:
        current_interest_score = DimVector.dot_product(
            normalized_profile_industries_vector(profile_interest_v),
            ticker_industry_v) / DimVector.norm(ticker_industry_v, order=1)

        interest_score = max(interest_score, current_interest_score)
        if current_interest_score > 0:
            interest_matches.append(profile_interest_v.name)

    return interest_score, interest_matches


def normalized_profile_industries_vector(vector: DimVector) -> DimVector:
    if len(vector.dims) == 0:
        return vector

    max_value = max(vector.values)
    min_value = min(vector.values)

    new_coordinates = {}
    denominator = 1.0 + sqrt(max_value) - sqrt(min_value)
    for (dim, value) in zip(vector.dims, vector.values):
        new_coordinates[dim] = (1.0 + sqrt(value) -
                                sqrt(min_value)) / denominator

    return DimVector(new_coordinates)


# RISK SIMILARITY SCORE

RISK_TO_SCORE_MAPPING = {1: 0.0, 2: 0.5, 3: 1.0}


def get_categories_risk_score(categories: DimVector, risk_mapping: Dict[str,
                                                                        int]):
    risk_sum = None
    categories_num = None
    for category in categories.dims:
        risk = risk_mapping.get(category, None)
        if risk is not None:
            if risk_sum is None:
                risk_sum = RISK_TO_SCORE_MAPPING[risk]
                categories_num = 1
            else:
                risk_sum += RISK_TO_SCORE_MAPPING[risk]
                categories_num += 1

    if risk_sum is None or categories_num is None:
        return None
    else:
        return risk_sum / categories_num


def get_risk_similarity(profile_categories: DimVector,
                        ticker_categories: DimVector,
                        risk_mapping: Dict[str, int]) -> float:
    profile_risk_score = get_categories_risk_score(profile_categories,
                                                   risk_mapping)
    ticker_risk_score = get_categories_risk_score(ticker_categories,
                                                  risk_mapping)

    if profile_risk_score is None or ticker_risk_score is None:
        return 0.0

    return 1.0 - abs(profile_risk_score - ticker_risk_score)


# CATEGORY SIMILARITY SCORE


def get_category_similarity(profile_category_v: DimVector,
                            ticker_category_v: DimVector) -> (float, List[str]):

    category_similarity = profile_category_v.cosine_similarity(ticker_category_v)

    category_matches = sorted(set(profile_category_v.dims).intersection(ticker_category_v.dims))
    category_matches = list(map(lambda category_id: int(category_id), category_matches))

    return category_similarity, category_matches


# GENERAL SIMILARITY SCORE


class MatchScoreComponent(Enum):
    RISK = "risk"
    CATEGORY = "category"
    INTEREST = "interest"


class SimilarityLevel(Enum):
    LOW = 0
    MID = 1
    HIGH = 2


EXPLANATION_CONFIG = {
    MatchScoreComponent.RISK: [(None, 0.3, SimilarityLevel.LOW),
                               (0.3, 0.7, SimilarityLevel.MID),
                               (0.7, None, SimilarityLevel.HIGH)],
    MatchScoreComponent.CATEGORY: [(None, 0.3, SimilarityLevel.LOW),
                                   (0.3, 0.7, SimilarityLevel.MID),
                                   (0.7, None, SimilarityLevel.HIGH)],
    MatchScoreComponent.INTEREST: [(None, 0.3, SimilarityLevel.LOW),
                                   (0.3, 0.7, SimilarityLevel.MID),
                                   (0.7, None, SimilarityLevel.HIGH)]
}


class MatchScoreExplanation:
    def __init__(
        self,
        risk_level: SimilarityLevel,
        risk_similarity: float,
        category_level: SimilarityLevel,
        category_matches: List[str],
        interest_level: SimilarityLevel,
        interest_matches: List[str]
    ):
        self.risk_level = risk_level
        self.risk_similarity = risk_similarity
        self.category_level = category_level
        self.category_matches = category_matches
        self.interest_level = interest_level
        self.interest_matches = interest_matches


class MatchScoreExplainer:
    def __init__(self, config):
        self.config = config

    def _apply_explanation_config(self, similarity,
                                  component) -> SimilarityLevel:

        for lower_bound, upper_bound, similarity_level in self.config[
                component]:
            if (not lower_bound or lower_bound <= similarity) and (
                    not upper_bound or upper_bound > similarity):
                return similarity_level

        return SimilarityLevel.LOW

    def explanation(self, risk_similarity,
                    category_similarity, category_matches,
                    interest_similarity, interest_matches
    ) -> MatchScoreExplanation:

        risk_level = self._apply_explanation_config(risk_similarity,
                                                    MatchScoreComponent.RISK)
        category_level = self._apply_explanation_config(
            category_similarity, MatchScoreComponent.CATEGORY)
        interest_level = self._apply_explanation_config(
            interest_similarity, MatchScoreComponent.INTEREST)

        return MatchScoreExplanation(risk_level, risk_similarity, category_level, category_matches,
                                     interest_level, interest_matches)


class MatchScore:
    def __init__(self, similarity: float, risk_similarity: float,
                 category_similarity: float, category_matches: List[str],
                 interest_similarity: float, interest_matches: List[str]):
        self.similarity = similarity

        self.risk_similarity = risk_similarity

        self.category_similarity = category_similarity
        self.category_matches = category_matches

        self.interest_similarity = interest_similarity
        self.interest_matches = interest_matches

        self.similarity_explainer = MatchScoreExplainer(EXPLANATION_CONFIG)

    def match_score(self):
        return round(self.similarity * 100)

    def explain(self) -> MatchScoreExplanation:
        return self.similarity_explainer.explanation(self.risk_similarity,
                                                     self.category_similarity,
                                                     self.category_matches,
                                                     self.interest_similarity,
                                                     self.interest_matches)


def profile_ticker_similarity(
        profile_categories: DimVector,
        ticker_categories: DimVector,
        risk_mapping: Dict[str, int],
        profile_interests: List[NamedDimVector],
        ticker_industries: DimVector,
) -> MatchScore:
    risk_weight = 1 / 3
    category_weight = 1 / 3
    interest_weight = 1 / 3

    risk_similarity = get_risk_similarity(profile_categories, ticker_categories, risk_mapping)
    (category_similarity, category_matches) = get_category_similarity(profile_categories, ticker_categories)
    (interest_similarity, interest_matches) = get_interest_similarity(profile_interests, ticker_industries)

    similarity = risk_weight * risk_similarity + category_weight * category_similarity + interest_weight * interest_similarity

    return MatchScore(similarity, risk_similarity, category_similarity, category_matches, interest_similarity, interest_matches)
