import logging

from common.hasura_function import HasuraAction

from psycopg2.extras import execute_values
from gainy.data_access.optimistic_lock import ConcurrentVersionUpdate
from gainy.recommendation.compute import ComputeRecommendationsAndPersist
from gainy.recommendation.repository import RecommendationRepository

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class SetRecommendationSettings(HasuraAction):

    def __init__(self):
        super().__init__("set_recommendation_settings", "profile_id")

    def apply(self, db_conn, input_params, headers):
        profile_id = input_params["profile_id"]
        interests = input_params["interests"]
        categories = input_params["categories"]
        recommended_collections_count = input_params.get(
            "recommended_collections_count", 0)

        recommendations_func = ComputeRecommendationsAndPersist(
            db_conn, profile_id)
        try:
            recommendations_func.get_and_persist(db_conn, max_tries=3)
        except ConcurrentVersionUpdate:
            pass

        self.set_interests(db_conn, profile_id, interests)
        self.set_categories(db_conn, profile_id, categories)

        recommendations_func = ComputeRecommendationsAndPersist(
            db_conn, profile_id)
        try:
            recommendations_func.get_and_persist(db_conn, max_tries=3)
        except ConcurrentVersionUpdate:
            pass

        repository = RecommendationRepository(db_conn)
        collections = repository.get_recommended_collections(
            profile_id, recommended_collections_count)

        return {
            "recommended_collections": [{
                "id": id,
                "uniq_id": uniq_id
            } for id, uniq_id in collections]
        }

    def set_interests(self, db_conn, profile_id, interests):
        with db_conn.cursor() as cursor:
            cursor.execute(
                "update app.profile_interests set skip_trigger = true where profile_id = %(profile_id)s",
                {"profile_id": profile_id})

            cursor.execute(
                "delete from app.profile_interests where profile_id = %(profile_id)s",
                {"profile_id": profile_id})

            execute_values(
                cursor,
                "INSERT INTO app.profile_interests(profile_id, interest_id, skip_trigger) VALUES %s",
                [(profile_id, interest_id, True) for interest_id in interests])

    def set_categories(self, db_conn, profile_id, categories):
        with db_conn.cursor() as cursor:
            cursor.execute(
                "update app.profile_categories set skip_trigger = true where profile_id = %(profile_id)s",
                {"profile_id": profile_id})

            cursor.execute(
                "delete from app.profile_categories where profile_id = %(profile_id)s",
                {"profile_id": profile_id})

            execute_values(
                cursor,
                "INSERT INTO app.profile_categories(profile_id, category_id, skip_trigger) VALUES %s",
                [(profile_id, category_id, True)
                 for category_id in categories])
