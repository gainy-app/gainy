from psycopg2.extras import execute_values

from common.hasura_function import HasuraTrigger
from recommendation.recommendation_action import get_top_by_match_score


class ChangeTop20Collection(HasuraTrigger):

    TOP_20_FOR_YOU_COLLECTION_ID = 232

    def __init__(self):
        super().__init__([
            "top_20_collection__profile_categories",
            "top_20_collection__profile_interests",
            "top_20_collection__profile_scoring_settings"
        ])

    def apply(self, db_conn, op, data):
        profile_id = self.get_profile_id(op, data)

        top_20_tickers_with_score = get_top_by_match_score(db_conn, profile_id, 20)
        top_20_tickers = [ticker[0] for ticker in top_20_tickers_with_score]

        self._update_personalized_collection(db_conn, profile_id, self.TOP_20_FOR_YOU_COLLECTION_ID, top_20_tickers)

    def get_profile_id(self, op, data):
        return data["new"]["profile_id"]

    def _update_personalized_collection(self, db_conn, profile_id, collection_id, ticker_list):
        with db_conn.cursor() as cursor:
            cursor.execute(
                """SELECT profile_id, collection_id FROM app.personalized_collection_sizes 
                 WHERE profile_id = %(profile_id)s AND collection_id = %(collection_id)s FOR UPDATE""",
                {"profile_id": profile_id, "collection_id": collection_id}
            )

            if not cursor.fetchone() is None:
                cursor.execute(
                    """DELETE FROM app.personalized_ticker_collections 
                    WHERE profile_id = %(profile_id)s AND collection_id = %(collection_id)s""",
                    {"profile_id": profile_id, "collection_id": collection_id}
                )

                cursor.execute(
                    """UPDATE app.personalized_collection_sizes SET size = %(size)s
                    WHERE profile_id = %(profile_id)s AND collection_id = %(collection_id)s""",
                    {"profile_id": profile_id, "collection_id": collection_id, "size": len(ticker_list)}
                )
            else:
                cursor.execute(
                    "INSERT INTO app.personalized_collection_sizes(profile_id, collection_id, size) "
                    "VALUES (%(profile_id)s, %(collection_id)s, %(size)s)",
                    {"profile_id": profile_id, "collection_id": collection_id, "size": len(ticker_list)}
                )

            execute_values(
                cursor,
                "INSERT INTO app.personalized_ticker_collections(profile_id, collection_id, symbol) VALUES %s",
                [(profile_id, collection_id, symbol) for symbol in ticker_list]
            )
