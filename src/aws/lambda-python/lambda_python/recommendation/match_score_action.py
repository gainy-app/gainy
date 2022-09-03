from abc import ABC

from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.recommendation.repository import RecommendationRepository


class AbstractMatchScoreAction(HasuraAction, ABC):

    def __init__(self, name, profile_id_param):
        super().__init__(name, profile_id_param)

    def read_match_scores(self, repository, profile_id, symbols):
        result = []
        for row in repository.read_ticker_match_scores(profile_id, symbols):
            result.append({
                **row,
                "is_match": True,  # Deprecated, will be removed
            })

        return result


# Deprecated: should read Match score from DB via GraphQL instead
class GetMatchScoreByTicker(AbstractMatchScoreAction):

    def __init__(self):
        super().__init__("get_match_score_by_ticker", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        db_conn = context_container.db_conn
        profile_id = input_params["profile_id"]
        ticker = input_params["symbol"]

        repository = context_container.recommendation_repository
        match_scores = self.read_match_scores(repository, profile_id, [ticker])

        if not match_scores:
            return None

        return match_scores[0]


# Deprecated: should read Match score from DB via GraphQL instead
class GetMatchScoreByTickerList(AbstractMatchScoreAction):

    def __init__(self):
        super().__init__("get_match_scores_by_ticker_list", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        db_conn = context_container.db_conn
        profile_id = input_params["profile_id"]
        tickers = input_params["symbols"]

        if len(tickers) == 0:
            return []

        repository = context_container.recommendation_repository
        return super().read_match_scores(repository, profile_id, tickers)


# Deprecated: should read Match score from DB via GraphQL instead
class GetMatchScoreByCollection(AbstractMatchScoreAction):

    def __init__(self):
        super().__init__("get_match_scores_by_collection", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        db_conn = context_container.db_conn
        profile_id = input_params["profile_id"]
        collection_id = input_params["collection_id"]

        repository = context_container.recommendation_repository
        collection_tickers = repository.read_collection_tickers(
            profile_id, collection_id)

        if len(collection_tickers) == 0:
            return []

        return super().read_match_scores(repository, profile_id,
                                         collection_tickers)
