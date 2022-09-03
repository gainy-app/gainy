from common.context_container import ContextContainer
from common.hasura_function import HasuraAction

from psycopg2.extras import execute_values
from gainy.data_access.db_lock import LockAcquisitionTimeout
from gainy.data_access.optimistic_lock import ConcurrentVersionUpdate
from gainy.recommendation.compute import ComputeRecommendationsAndPersist
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

        logging_extra = {
            'function': 'SetRecommendationSettings',
            'profile_id': profile_id,
            'interests': interests,
            'categories': categories,
        }

        self.set_interests(db_conn, profile_id, interests)
        self.set_categories(db_conn, profile_id, categories)

        repository = context_container.recommendation_repository
        recommendations_func = ComputeRecommendationsAndPersist(
            repository, profile_id)
        old_version = recommendations_func.load_version()
        try:
            recommendations_func.get_and_persist(max_tries=2)

            new_version = recommendations_func.load_version()
            logger.info('Calculated Match Scores',
                        extra={
                            **logging_extra,
                            'old_version': old_version.recommendations_version,
                            'new_version': new_version.recommendations_version,
                        })
        except (LockAcquisitionTimeout, ConcurrentVersionUpdate) as e:
            logger.exception(e, extra=logging_extra)

        if recommended_collections_count is not None:
            collections = repository.get_recommended_collections(
                profile_id, recommended_collections_count)
        else:
            collections = []

        return {
            "recommended_collections": [{
                "id": id,
                "uniq_id": uniq_id
            } for id, uniq_id in collections]
        }

    def set_interests(self, db_conn, profile_id, interests):
        if interests is None:
            return

        with db_conn.cursor() as cursor:
            cursor.execute(
                "delete from app.profile_interests where profile_id = %(profile_id)s",
                {"profile_id": profile_id})

            execute_values(
                cursor,
                "INSERT INTO app.profile_interests(profile_id, interest_id, skip_trigger) VALUES %s",
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
                "INSERT INTO app.profile_categories(profile_id, category_id, skip_trigger) VALUES %s",
                [(profile_id, category_id, True)
                 for category_id in categories])
