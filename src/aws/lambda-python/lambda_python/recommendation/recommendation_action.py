from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.recommendation.serivce import format_collections
from gainy.utils import get_logger

logger = get_logger(__name__)


class GetRecommendedCollections(HasuraAction):

    def __init__(self):
        super().__init__("get_recommended_collections", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params["profile_id"]
        limit = input_params.get("limit", 30)

        try:
            collections = context_container.recommendation_service.get_recommended_collections(
                profile_id, limit)

            return format_collections(collections)
        except Exception as e:
            logger.exception('get_recommended_collections: error %s',
                             e,
                             extra={
                                 'profile_id': profile_id,
                             })
