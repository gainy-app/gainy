from enum import Enum
from math import sqrt
from typing import Dict

from recommendation.core.dim_vector import DimVector

# IS MATCH


def is_match(profile_category_vector: DimVector,
             ticker_category_vector: DimVector) -> bool:
    profile_categories = profile_category_vector.dims
    ticker_categories = ticker_category_vector.dims

    return len(set(profile_categories).intersection(ticker_categories)) > 0


# INDUSTRY SIMILARITY SCORE


def get_interest_similarity(profile_industries: DimVector,
                            ticker_industries: DimVector) -> float:
    norm_profile_industries = normalized_profile_industries_vector(
        profile_industries)
    return DimVector.dot_product(norm_profile_industries,
                                 ticker_industries) / DimVector.norm(
                                     ticker_industries, order=1)


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


def get_category_similarity(profile_categories: DimVector,
                            ticker_categories: DimVector) -> float:
    return profile_categories.cosine_similarity(ticker_categories)


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
    def __init__(self, risk_level: SimilarityLevel,
                 category_level: SimilarityLevel,
                 interest_level: SimilarityLevel):
        self.risk_level = risk_level
        self.category_level = category_level
        self.interest_level = interest_level


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

    def explanation(self, risk_similarity, category_similarity,
                    interest_similarity) -> MatchScoreExplanation:

        risk_level = self._apply_explanation_config(risk_similarity,
                                                    MatchScoreComponent.RISK)
        category_level = self._apply_explanation_config(
            category_similarity, MatchScoreComponent.CATEGORY)
        interest_level = self._apply_explanation_config(
            interest_similarity, MatchScoreComponent.INTEREST)

        return MatchScoreExplanation(risk_level, category_level,
                                     interest_level)


class MatchScore:
    def __init__(self, similarity: float, risk_similarity: float,
                 category_similarity: float, interest_similarity: float):
        self.similarity = similarity

        self.risk_similarity = risk_similarity
        self.category_similarity = category_similarity
        self.interest_similarity = interest_similarity

        self.similarity_explainer = MatchScoreExplainer(EXPLANATION_CONFIG)

    def match_score(self):
        return round(self.similarity * 100)

    def explain(self) -> MatchScoreExplanation:
        return self.similarity_explainer.explanation(self.risk_similarity,
                                                     self.category_similarity,
                                                     self.interest_similarity)


def profile_ticker_similarity(
    profile_categories: DimVector,
    ticker_categories: DimVector,
    risk_mapping: Dict[str, int],
    profile_industries: DimVector,
    ticker_industries: DimVector,
) -> MatchScore:
    risk_weight = 1 / 3
    category_weight = 1 / 3
    interest_weight = 1 / 3

    risk_similarity = get_risk_similarity(profile_categories,
                                          ticker_categories, risk_mapping)
    category_similarity = get_category_similarity(profile_categories,
                                                  ticker_categories)
    interest_similarity = get_interest_similarity(profile_industries,
                                                  ticker_industries)

    similarity = risk_weight * risk_similarity + category_weight * category_similarity + interest_weight * interest_similarity

    return MatchScore(similarity, risk_similarity, category_similarity,
                      interest_similarity)
