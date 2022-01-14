import json
import time

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
        start_total = time.time()

        profile_id = self.get_profile_id(op, data)

        tickers_with_match_score = get_top_by_match_score(db_conn, profile_id)

        start = time.time()
        self._save_match_score(db_conn, profile_id, tickers_with_match_score)
        print(f"LAMBDA_PROFILE: Save match scores to DB: {time.time() - start} ms")

        start = time.time()
        top_20_tickers = [ticker[0] for ticker in tickers_with_match_score[:20]]
        update_personalized_collection(db_conn, profile_id,
                                       TOP_20_FOR_YOU_COLLECTION_ID,
                                       top_20_tickers)
        print(f"LAMBDA_PROFILE: Update top-20 collection: {time.time() - start} ms")

        print(f"LAMBDA_PROFILE: Total execution time: {time.time() - start_total} ms")


    def _save_match_score(self, db_conn, profile_id, tickers_with_match_score):
        match_score_json_list = json.dumps(list(map(SetTop20Collection._match_score_as_json, tickers_with_match_score)))

        with db_conn.cursor() as cursor:
            cursor.execute(
                """INSERT INTO app.profile_ticker_match(profile, match_score_json)
                VALUES (%(profile_id)s, %(match_score_json)s)
                ON CONFLICT (profile) DO UPDATE SET match_score_json = EXCLUDED.match_score_json""",
                {
                    "profile_id": profile_id,
                    "match_score_json": match_score_json_list
                }
            )

    @staticmethod
    def _match_score_as_json(ticker_with_match_score):
        explanation = ticker_with_match_score[1].explain()

        return {
            "symbol": ticker_with_match_score[0],
            "match_score": ticker_with_match_score[1].match_score(),
            "fits_risk": explanation.risk_level.value,
            "risk_similarity": explanation.risk_similarity,
            "fits_categories": explanation.category_level.value,
            "category_matches": explanation.category_matches,
            "fits_interests": explanation.interest_level.value,
            "interest_matches": explanation.interest_matches
        }

    def get_profile_id(self, op, data):
        payload = self._extract_payload(data)
        return payload["profile_id"]
