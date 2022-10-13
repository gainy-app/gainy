from common.context_container import ContextContainer
from common.hasura_function import HasuraTrigger
from gainy.recommendation.compute import ComputeRecommendationsAndPersist
from gainy.data_access.optimistic_lock import ConcurrentVersionUpdate
from gainy.data_access.db_lock import LockAcquisitionTimeout
from gainy.utils import get_logger

logger = get_logger(__name__)


# Deprecated
class SetRecommendations(HasuraTrigger):

    def __init__(self):
        super().__init__([
            "recommendations__profile_categories",
            "recommendations__profile_interests"
        ])

    def apply(self, op, data, context_container: ContextContainer):
        db_conn = context_container.db_conn
        profile_id = self.get_profile_id(data)
        payload = self._extract_payload(data)
        category_id = payload.get("category_id")
        interest_id = payload.get("interest_id")

        logging_extra = {
            'function': 'SetRecommendations',
            'profile_id': profile_id,
            'category_id': category_id,
            'interest_id': interest_id,
        }

        skip_trigger = self.skip_category_update(
            db_conn, profile_id, category_id) or self.skip_interest_update(
                db_conn, profile_id, interest_id)
        if skip_trigger:
            logger.info('Skipped calculating Match Scores',
                        extra=logging_extra)
            return

        repository = context_container.recommendation_repository
        recommendations_func = ComputeRecommendationsAndPersist(
            repository, profile_id)
        old_version = recommendations_func.load_version()

        try:
            recommendations_func.execute(max_tries=5)
            new_version = recommendations_func.load_version()
            logger.info('Calculated Match Scores',
                        extra={
                            **logging_extra,
                            'old_version': old_version.recommendations_version,
                            'new_version': new_version.recommendations_version,
                        })
        except (LockAcquisitionTimeout, ConcurrentVersionUpdate) as e:
            """
            Sometimes hasura executes triggers in bursts (5-20 executions per 1-2 seconds).
            In this case the first execution, that acquires the lock, updates recommendations,
            and all others will fail with this exception. In this case we just need to make sure
            that an update will run with fresh data - we'll allow it to calculate in up to 12 seconds.
            """
            logger.warning('Match Score Calculation failed: %s',
                           e,
                           extra=logging_extra)

    def skip_category_update(self, db_conn, profile_id, category_id):
        if category_id is None:
            return False

        with db_conn.cursor() as cursor:
            cursor.execute(
                "select skip_trigger from app.profile_categories where profile_id = %(profile_id)s and category_id = %(category_id)s",
                {
                    "profile_id": profile_id,
                    "category_id": category_id
                })
            row = cursor.fetchone()

        return row and row[0]

    def skip_interest_update(self, db_conn, profile_id, interest_id):
        if interest_id is None:
            return False

        with db_conn.cursor() as cursor:
            cursor.execute(
                "select skip_trigger from app.profile_interests where profile_id = %(profile_id)s and interest_id = %(interest_id)s",
                {
                    "profile_id": profile_id,
                    "interest_id": interest_id
                })
            row = cursor.fetchone()

        return row and row[0]

    def get_profile_id(self, data):
        payload = self._extract_payload(data)
        return payload["profile_id"]

    def get_allowed_profile_ids(self, op, data):
        return self.get_profile_id(data)
