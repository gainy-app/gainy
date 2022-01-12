import json

from common.hasura_function import HasuraAction
from recommendation.collection_ranking import TFIDFWithNorm1_5CollectionRanking, MatchScoreCollectionRanking
from recommendation.data_access import read_profile_industry_vector, \
    read_industry_corpus_size, read_industry_frequencies, read_all_collection_industry_vectors, is_collection_enabled, \
    read_all_ticker_category_and_industry_vectors, read_profile_category_vector, read_profile_interest_vectors, \
    read_categories_risks, read_collection_tickers
from recommendation.match_score.match_score import profile_ticker_similarity
from recommendation.top_for_you import TOP_20_FOR_YOU_COLLECTION_ID


class GetRecommendedCollections(HasuraAction):

    def __init__(self):
        super().__init__("get_recommended_collections", "profile_id")
        self.ranking = MatchScoreCollectionRanking()

    def apply(self, db_conn, input_params, headers):
        profile_id = input_params["profile_id"]

        document_frequencies = read_industry_frequencies(db_conn)
        corpus_size = read_industry_corpus_size(db_conn)

        collection_vs = read_all_collection_industry_vectors(db_conn)
        profile_v = read_profile_industry_vector(db_conn, profile_id)

        ticker_vs_list = read_all_ticker_category_and_industry_vectors(db_conn)
        profile_category_v = read_profile_category_vector(db_conn, profile_id)
        profile_interest_vs = read_profile_interest_vectors(db_conn, profile_id)

        risk_mapping = read_categories_risks(db_conn)

        ticker_match_scores = {}
        for ticker_vs in ticker_vs_list:
            match_score = profile_ticker_similarity(profile_category_v,
                                                    ticker_vs[1],
                                                    risk_mapping,
                                                    profile_interest_vs,
                                                    ticker_vs[0])
            ticker_match_scores[ticker_vs[0].name] = match_score.match_score()

        collection_tickers = {}
        for collection_v in collection_vs:
            collection_tickers[collection_v.name] = read_collection_tickers(db_conn, profile_id, collection_v.name)

        ranked_collections = self.ranking.rank(profile_v,
                                               collection_vs,
                                               df=document_frequencies,
                                               size=corpus_size,
                                               ticker_match_scores=ticker_match_scores,
                                               collection_tickers=collection_tickers
                                               )

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
