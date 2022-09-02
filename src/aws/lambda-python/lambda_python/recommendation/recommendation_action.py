from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.data_access.db_lock import LockAcquisitionTimeout
from gainy.data_access.optimistic_lock import ConcurrentVersionUpdate
from gainy.recommendation.compute import ComputeRecommendationsAndPersist
from gainy.recommendation.repository import RecommendationRepository, RecommendedCollectionAlgorithm
from gainy.utils import get_logger

logger = get_logger(__name__)


class GetRecommendedCollections(HasuraAction):

    def __init__(self):
        super().__init__("get_recommended_collections", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        db_conn = context_container.db_conn
        try:
            profile_id = input_params["profile_id"]
            limit = input_params.get("limit", 30)
            logging_extra = {
                'profile_id': profile_id,
            }

            logger.info('get_recommended_collections: start',
                        extra=logging_extra)

            repository = RecommendationRepository(db_conn)
            collections = repository.get_recommended_collections(
                profile_id, limit)

            if not len(collections):
                logger.info('get_recommended_collections: update_match_scores',
                            extra=logging_extra)
                self.update_match_scores(db_conn, profile_id)
                collections = repository.get_recommended_collections(
                    profile_id, limit)

            if not len(collections):
                logger.warning('get_recommended_collections: use top_favorite',
                               extra=logging_extra)
                collections = repository.get_recommended_collections(
                    profile_id, limit,
                    RecommendedCollectionAlgorithm.TOP_FAVORITED)

            if not len(collections):
                logger.error(
                    'get_recommended_collections: no collections to recommend',
                    extra=logging_extra)

            return [{
                "id": id,
                "uniq_id": uniq_id
            } for id, uniq_id in collections]

        except Exception as e:
            logger.exception('get_recommended_collections: error %s',
                             e,
                             extra=logging_extra)

    def update_match_scores(self, db_conn, profile_id):
        recommendations_func = ComputeRecommendationsAndPersist(
            RecommendationRepository(db_conn), profile_id)
        try:
            recommendations_func.get_and_persist(max_tries=2)
        except (LockAcquisitionTimeout, ConcurrentVersionUpdate):
            pass
