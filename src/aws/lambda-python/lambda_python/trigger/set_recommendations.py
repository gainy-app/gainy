from common.hasura_function import HasuraTrigger
from gainy.recommendation.compute import ComputeRecommendationsAndPersist
from gainy.data_access.optimistic_lock import ConcurrentVersionUpdate
from gainy.data_access.db_lock import LockManager, ResourceType, DatabaseLock, LockAcquisitionTimeout
from service.logging import get_logger

logger = get_logger(__name__)


class SetRecommendations(HasuraTrigger):

    def __init__(self):
        super().__init__([
            "recommendations__profile_categories",
            "recommendations__profile_interests"
        ])

    def apply(self, db_conn, op, data):
        profile_id = self.get_profile_id(op, data)

        recommendations_func = ComputeRecommendationsAndPersist(
            db_conn, profile_id)
        old_version = recommendations_func.load_version(db_conn)

        try:
            recommendations_func.get_and_persist(db_conn, max_tries=5)
            new_version = recommendations_func.load_version(db_conn)
            logger.info(
                f'Calculated Match Scores for user {profile_id}, version {old_version.recommendations_version} => {new_version.recommendations_version}'
            )
        except (LockAcquisitionTimeout, ConcurrentVersionUpdate):
            """
            Sometimes hasura executes triggers in bursts (5-20 executions per 1-2 seconds).
            In this case the first execution, that acquires the lock, updates recommendations,
            and all others will fail with this exception. In this case we just need to make sure
            that an update will run with fresh data - we'll allow it to calculate in up to 12 seconds.
            """
            pass

    def get_profile_id(self, op, data):
        payload = self._extract_payload(data)
        return payload["profile_id"]
