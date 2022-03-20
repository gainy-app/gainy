import json
from operator import itemgetter
from common.hasura_function import HasuraAction
from gainy.recommendation import TOP_20_FOR_YOU_COLLECTION_ID
from gainy.recommendation.compute import ComputeRecommendationsAndPersist
from gainy.recommendation.repository import RecommendationRepository
from service.logging import get_logger

logger = get_logger(__name__)


class GetRecommendedCollections(HasuraAction):

    def __init__(self):
        super().__init__("get_recommended_collections", "profile_id")

    def apply(self, db_conn, input_params, headers):
        profile_id = input_params["profile_id"]
        limit = input_params.get("limit", 30)

        force = input_params.get("force", False)
        if force:
            recommendations_func = ComputeRecommendationsAndPersist(
                db_conn, profile_id)
            try:
                recommendations_func.get_and_persist(db_conn, max_tries=7)
            except ConcurrentVersionUpdate:
                pass

        repository = RecommendationRepository(db_conn)
        sorted_collection_match_scores = repository.read_sorted_collection_match_scores(
            profile_id, limit)
        sorted_collections_ids = list(
            map(itemgetter(0), sorted_collection_match_scores))

        # Add `top-20 for you` collection as the top item
        is_top_20_enabled = repository.is_collection_enabled(
            profile_id, TOP_20_FOR_YOU_COLLECTION_ID)
        if is_top_20_enabled:
            sorted_collections_ids = [TOP_20_FOR_YOU_COLLECTION_ID
                                      ] + sorted_collections_ids

        logger.info('get_recommended_collections ' +
                    json.dumps({
                        'profile_id': profile_id,
                        'collections': sorted_collections_ids,
                    }))

        return [{"id": id} for id in sorted_collections_ids]
