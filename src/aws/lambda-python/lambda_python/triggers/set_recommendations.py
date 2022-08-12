from common.hasura_function import HasuraTrigger
from gainy.recommendation.compute import ComputeRecommendationsAndPersist
from gainy.data_access.optimistic_lock import ConcurrentVersionUpdate
from gainy.data_access.db_lock import LockAcquisitionTimeout
from gainy.utils import get_logger

logger = get_logger(__name__)


class SetRecommendations(HasuraTrigger):

    def __init__(self):
        super().__init__([
            "recommendations__profile_categories",
            "recommendations__profile_interests"
        ])

    def apply(self, db_conn, op, data):
        profile_id = self.get_profile_id(data)

        recommendations_func = ComputeRecommendationsAndPersist(
            db_conn, profile_id)
        old_version = recommendations_func.load_version(db_conn)

        try:
            recommendations_func.get_and_persist(db_conn, max_tries=3)
            new_version = recommendations_func.load_version(db_conn)
            logger.info('Calculated Match Scores',
                        extra={
                            'profile_id': profile_id,
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
                           extra={
                               'profile_id': profile_id,
                           })

    def get_profile_id(self, data):
        payload = self._extract_payload(data)
        return payload["profile_id"]

    def get_allowed_profile_ids(self, op, data):
        return self.get_profile_id(data)
