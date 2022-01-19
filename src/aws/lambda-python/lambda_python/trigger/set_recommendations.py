from typing import List, Tuple

from common.hasura_function import HasuraTrigger
from recommendation.models import MatchScoreModel
from recommendation.repository import RecommendationRepository
from recommendation.match_score import MatchScore, profile_ticker_similarity
from recommendation import TOP_20_FOR_YOU_COLLECTION_ID


class SetRecommendations(HasuraTrigger):

    def __init__(self):
        super().__init__([
            "recommendations__profile_categories",
            "recommendations__profile_interests",
            "recommendations__profile_scoring_settings"
        ])

    def apply(self, db_conn, op, data):
        profile_id = self.get_profile_id(op, data)

        repository = RecommendationRepository(db_conn)

        tickers_with_match_score = self.get_and_sort_by_match_score(
            repository, profile_id)

        match_score_entities = [
            MatchScoreModel(profile_id, ticker, match_score)
            for ticker, match_score in tickers_with_match_score
        ]
        repository.persist(db_conn, match_score_entities)

        top_20_tickers = [
            ticker[0] for ticker in tickers_with_match_score[:20]
        ]
        repository.update_personalized_collection(
            profile_id, TOP_20_FOR_YOU_COLLECTION_ID, top_20_tickers)

    def get_and_sort_by_match_score(
            self,
            repository,
            profile_id: int,
            top_k: int = None) -> List[Tuple[str, MatchScore]]:
        profile_category_v = repository.read_profile_category_vector(
            profile_id)
        profile_interest_vs = repository.read_profile_interest_vectors(
            profile_id)

        risk_mapping = repository.read_categories_risks()

        ticker_vs_list = repository.read_all_ticker_category_and_industry_vectors(
        )

        match_score_list = []
        for ticker_vs in ticker_vs_list:
            match_score = profile_ticker_similarity(profile_category_v,
                                                    ticker_vs[1], risk_mapping,
                                                    profile_interest_vs,
                                                    ticker_vs[0])
            match_score_list.append((ticker_vs[0].name, match_score))

        # Uses minus `match_score` to correctly sort the list by both score and symbol
        match_score_list.sort(key=lambda m: (-m[1].match_score(), m[0]))

        return match_score_list[:top_k] if top_k else match_score_list

    def get_profile_id(self, op, data):
        payload = self._extract_payload(data)
        return payload["profile_id"]
