from abc import ABC, abstractmethod
import math

from recommendation.core.dim_vector import DimVector
from recommendation.core.normalization import double_normalization_k


class RankedItem:
    def __init__(self, item, rank_score: float):
        self.item = item
        self.rank_score = rank_score


class CollectionRanking(ABC):
    """
    An abstract class, which represents a collection ranking algorithm. Collection ranking is calculated
    using industry vector for user's profile (calculated based on profile interests) and  industry vector
    for collections (calculated based on tickers in that collection).

    Additional parameters might be passed to the algorithm - e.g. industry frequencies for TF*IDF-based
    algorithm.
    """
    def rank(self, profile_v: DimVector, collection_vs: list[DimVector],
             **params) -> list[RankedItem]:
        ranked_items = list(
            map(
                lambda collection_v: RankedItem(
                    collection_v,
                    self.rank_score(profile_v, collection_v, **params)),
                collection_vs))
        ranked_items.sort(key=lambda item: item.rank_score, reverse=True)
        return ranked_items

    @abstractmethod
    def rank_score(self, profile_v: DimVector, collection_v: DimVector,
                   **params) -> float:
        pass


class TFCollectionRanking(CollectionRanking):
    """
    TFCollectionRanking is a baseline ranking method. It is based on term frequency and cosine similarity
    with no normalization.
    """
    def rank_score(self, profile_v: DimVector, collection_v: DimVector,
                   **params) -> float:
        return profile_v.cosine_similarity(collection_v)


class TFIDFWithNorm1_5CollectionRanking(CollectionRanking):
    """
    TFIDFWithNorm1_5CollectionRanking is an enhanced ranking method, which shows better results - both in terms of
    Mean Average Precision comparing to favourite collections for existing users and human evaluation of ranking
    results.

    Ranking algorithm:

    1. Profile vector (industries):
    1.1 Term frequency vector is normalized using :func:`double_normalization_k` to smooth frequencies
    of industries related to user's interests.
    1.2 Inverted Document Frequency is not applied to the normalized profile vector. It reflects the intuition
    that frequency of industries in collections should not affect weights of interests for a particular user.

    2. Collection vector (industries):
    2.1. Term frequency vector is weighted using :func:`log` to smooth frequencies of industries.
    2.2. The smoothed term frequency vector is multiplied by the log of Inverted Document Frequency (IDF).
    IDF for an industry is calculated as the frequency of the industry tickers in all collections over
    the total number of tickers in all collections.

    3. Similarity measure:
    3.1 Similarity measure is as a modification of cosine similarity, where norm of vectors is taken
    in L_{1.5} space (comparing to more typical L_{2} norm in Euclidean space). This similarity measure
    gives more intuitive ranking results, as well as better metrics for the existing users.
    """
    def rank_score(self, profile_v: DimVector, collection_v: DimVector,
                   **params) -> float:
        df = params['df']
        corpus_size = params['size']

        norm_profile_v = self._normalize_profile_vector(profile_v)
        norm_collection_v = self._normalize_collection_vector(
            collection_v, df, corpus_size)

        return self._similarity(norm_profile_v, norm_collection_v)

    @staticmethod
    def _similarity(norm_profile_v, norm_collection_v):
        return norm_profile_v.cosine_similarity(norm_collection_v,
                                                norm_order=1.5)

    @staticmethod
    def _normalize_profile_vector(profile_v: DimVector) -> DimVector:
        new_coordinates = {}
        for (dim, value) in zip(profile_v.dims,
                                double_normalization_k(profile_v).values):
            tf = value
            idf = 1

            new_coordinates[dim] = tf * idf

        return DimVector(new_coordinates)

    @staticmethod
    def _normalize_collection_vector(collection_v: DimVector, df,
                                     corpus_size) -> DimVector:
        new_coordinates = {}
        for (dim, value) in zip(collection_v.dims, collection_v.values):
            tf = math.log(1 + value)
            idf = math.log(float(corpus_size) / (1 + df.get(int(dim), 0)))

            new_coordinates[dim] = tf * idf

        return DimVector(new_coordinates)
