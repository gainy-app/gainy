import json
from operator import itemgetter

from common.hasura_function import HasuraAction
from recommendation import TOP_20_FOR_YOU_COLLECTION_ID
from recommendation.repository import RecommendationRepository


class GetRecommendedCollections(HasuraAction):

    def __init__(self):
        super().__init__("get_recommended_collections", "profile_id")

    def apply(self, db_conn, input_params, headers):
        profile_id = input_params["profile_id"]
        limit = input_params.get("limit", 30)

        repository = RecommendationRepository(db_conn)
        sorted_collection_match_scores = repository.read_sorted_collection_match_scores(
            profile_id, limit)
        sorted_collections_ids = list(
            map(itemgetter(0), sorted_collection_match_scores))

        # Add `top-20 for you` collection as the top item
        ranked_collections_ids = [TOP_20_FOR_YOU_COLLECTION_ID
                                  ] + sorted_collections_ids

        print('get_recommended_collections ' +
              json.dumps({
                  'profile_id': profile_id,
                  'collections': ranked_collections_ids,
              }))

        return [{"id": id} for id in ranked_collections_ids]
