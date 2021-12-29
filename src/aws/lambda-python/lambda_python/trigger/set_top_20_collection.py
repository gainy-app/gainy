from common.hasura_function import HasuraTrigger
from recommendation.data_access import update_personalized_collection
from recommendation.top_for_you import get_top_by_match_score, TOP_20_FOR_YOU_COLLECTION_ID


class SetTop20Collection(HasuraTrigger):
    def __init__(self):
        super().__init__([
            "top_20_collection__profile_categories",
            "top_20_collection__profile_interests",
            "top_20_collection__profile_scoring_settings"
        ])

    def apply(self, db_conn, op, data):
        profile_id = self.get_profile_id(op, data)

        top_20_tickers_with_score = get_top_by_match_score(
            db_conn, profile_id, 20)
        top_20_tickers = [ticker[0] for ticker in top_20_tickers_with_score]

        update_personalized_collection(db_conn, profile_id,
                                       TOP_20_FOR_YOU_COLLECTION_ID,
                                       top_20_tickers)

    def get_profile_id(self, op, data):
        payload = self._extract_payload(data)
        return payload["profile_id"]
