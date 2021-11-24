import json

from common.hasura_function import HasuraAction
from recommendation.collection_ranking import TFIDFWithNorm1_5CollectionRanking
from recommendation.data_access import read_profile_industry_vector, \
    read_industry_corpus_size, read_industry_frequencies, read_all_collection_industry_vectors, is_collection_enabled
from recommendation.top_for_you import TOP_20_FOR_YOU_COLLECTION_ID


class GetRecommendedCollections(HasuraAction):
    def __init__(self):
        super().__init__("get_recommended_collections", "profile_id")
        self.ranking = TFIDFWithNorm1_5CollectionRanking()

    def apply(self, db_conn, input_params):
        profile_id = input_params["profile_id"]

        document_frequencies = read_industry_frequencies(db_conn)
        corpus_size = read_industry_corpus_size(db_conn)

        collection_vs = read_all_collection_industry_vectors(db_conn)
        profile_v = read_profile_industry_vector(db_conn, profile_id)

        ranked_collections = self.ranking.rank(profile_v,
                                               collection_vs,
                                               df=document_frequencies,
                                               size=corpus_size)

        ranked_collections_ids = [c_v.item.name for c_v in ranked_collections]

        # Add `top-20 for you` collection as the top item
        is_top_20_enabled = is_collection_enabled(
            db_conn, profile_id, TOP_20_FOR_YOU_COLLECTION_ID)
        if is_top_20_enabled:
            ranked_collections_ids = [TOP_20_FOR_YOU_COLLECTION_ID
                                      ] + ranked_collections_ids

        print('get_recommended_collections ' +
              json.dumps({
                  'profile_id': profile_id,
                  'collections': ranked_collections_ids,
              }))

        return [{"id": id} for id in ranked_collections_ids]
