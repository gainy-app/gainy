from enum import Enum
from math import sqrt
from typing import Dict, List

from recommendation.dim_vector import DimVector

# IS MATCH


def is_match(profile_category_vector: DimVector,
             ticker_category_vector: DimVector) -> bool:
    profile_categories = profile_category_vector.dims
    ticker_categories = ticker_category_vector.dims

    return len(set(profile_categories).intersection(ticker_categories)) > 0


# INDUSTRY SIMILARITY SCORE


def get_industry_similarity(profile_industries: DimVector,
                            ticker_industries: DimVector) -> float:
    return DimVector.dot_product(
        normalized_profile_industries_vector(profile_industries),
        ticker_industries)


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

    return 1.0 - sqrt((profile_risk_score - ticker_risk_score) *
                      (profile_risk_score - ticker_risk_score))


# CATEGORY SIMILARITY SCORE


def get_category_similarity(profile_categories: DimVector,
                            ticker_categories: DimVector) -> float:
    return profile_categories.cosine_similarity(ticker_categories)


# GENERAL SIMILARITY SCORE


class MatchScoreComponent(Enum):
    RISK = "risk"
    CATEGORY = "category"
    INDUSTRY = "industry"


class MatchScoreExplanation:
    def __init__(self, component: MatchScoreComponent, fits: bool,
                 description: str):
        self.component = component
        self.fits = fits
        self.description = description


EXPLANATION_CONFIG = {
    MatchScoreComponent.RISK:
    [(0.0, 0.3, False, "Doesn't fit your risk profile"),
     (0.7, 1.0, True, "Fits your risk profile")],
    MatchScoreComponent.CATEGORY:
    [(0.0, 0.3, False, "Doesn't fit your preferred categories"),
     (0.7, 1.0, True, "Fits your preferred categories")],
    MatchScoreComponent.INDUSTRY:
    [(0.0, 0.3, False, "Doesn't fit your interests"),
     (0.7, 1.0, True, "Fits your interests")]
}


# TODO: change explanation approach
class MatchScoreExplainer:
    def __init__(self, config):
        self.config = config

    def __apply_explanation_config(self, similarity,
                                   component) -> List[MatchScoreExplanation]:
        explanations = []
        for lower_bound, upper_bound, fits, description in self.config[
                component]:
            if lower_bound <= similarity <= upper_bound:
                explanations.append(
                    MatchScoreExplanation(component, fits, description))

        return explanations

    def explain(self, risk_similarity, category_similarity,
                industry_similarity) -> List[MatchScoreExplanation]:
        explanations = []

        explanations += self.__apply_explanation_config(
            risk_similarity, MatchScoreComponent.RISK)
        explanations += self.__apply_explanation_config(
            category_similarity, MatchScoreComponent.CATEGORY)
        explanations += self.__apply_explanation_config(
            industry_similarity, MatchScoreComponent.INDUSTRY)

        return explanations


class MatchScore:
    def __init__(self, similarity: float, risk_similarity: float,
                 category_similarity: float, industry_similarity: float):
        self.similarity = similarity

        self.risk_similarity = risk_similarity
        self.category_similarity = category_similarity
        self.industry_similarity = industry_similarity

        self.similarity_explainer = MatchScoreExplainer(EXPLANATION_CONFIG)

    def explain(self, fits_only: bool = False) -> List[MatchScoreExplanation]:
        explanations = self.similarity_explainer.explain(
            self.risk_similarity, self.category_similarity,
            self.industry_similarity)

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

    risk_similarity = get_risk_similarity(profile_categories,
                                          ticker_categories, risk_mapping)
    category_similarity = get_category_similarity(profile_categories,
                                                  ticker_categories)
    industry_similarity = get_industry_similarity(profile_industries,
                                                  ticker_industries)

    similarity = risk_weight * risk_similarity + category_weight * category_similarity + industry_weight * industry_similarity

    return MatchScore(similarity, risk_similarity, category_similarity,
                      industry_similarity)
