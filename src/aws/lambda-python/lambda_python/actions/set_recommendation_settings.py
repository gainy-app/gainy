from common.context_container import ContextContainer
from common.hasura_function import HasuraAction

from psycopg2.extras import execute_values
from gainy.recommendation.serivce import format_collections
from gainy.utils import get_logger

logger = get_logger(__name__)


class SetRecommendationSettings(HasuraAction):

    def __init__(self):
        super().__init__("set_recommendation_settings", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        db_conn = context_container.db_conn
        profile_id = input_params["profile_id"]
        interests = input_params.get("interests")
        categories = input_params.get("categories")
        recommended_collections_count = input_params.get(
            "recommended_collections_count")

        self.set_interests(db_conn, profile_id, interests)
        self.set_categories(db_conn, profile_id, categories)

        service = context_container.recommendation_service
        service.compute_match_score(profile_id)

        if recommended_collections_count is not None and recommended_collections_count > 0:
            collections = service.get_recommended_collections(
                profile_id, recommended_collections_count)
        else:
            collections = []

        return {"recommended_collections": format_collections(collections)}

    def set_interests(self, db_conn, profile_id, interests):
        if interests is None:
            return

        with db_conn.cursor() as cursor:
            cursor.execute(
                "delete from app.profile_interests where profile_id = %(profile_id)s",
                {"profile_id": profile_id})

            execute_values(
                cursor,
                "INSERT INTO app.profile_interests(profile_id, interest_id, skip_trigger) VALUES %s ON CONFLICT DO NOTHING",
                [(profile_id, interest_id, True) for interest_id in interests])

    def set_categories(self, db_conn, profile_id, categories):
        if categories is None:
            return

        with db_conn.cursor() as cursor:
            cursor.execute(
                "delete from app.profile_categories where profile_id = %(profile_id)s",
                {"profile_id": profile_id})

            execute_values(
                cursor,
                "INSERT INTO app.profile_categories(profile_id, category_id, skip_trigger) VALUES %s ON CONFLICT DO NOTHING",
                [(profile_id, category_id, True)
                 for category_id in categories])
