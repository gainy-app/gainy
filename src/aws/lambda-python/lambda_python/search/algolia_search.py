from abc import ABC

from algoliasearch.search_client import SearchClient

from common.hasura_function import HasuraAction


class SearchAction(HasuraAction, ABC):

    def __init__(
            self,
            action_name,
            algolia_app_id,
            algolia_search_api_key,
            tickers_index,
            attributes_to_retrieve
    ):
        super().__init__(action_name)
        search_client = SearchClient.create(algolia_app_id, algolia_search_api_key)
        self.search_index = search_client.init_index(tickers_index)
        self.attributes_to_retrieve = attributes_to_retrieve

    def apply(self, db_conn, input_params):
        query = input_params["query"]
        offset = input_params.get("offset", 0)
        limit = input_params.get("limit", 10)

        result = self.search_index.search(query, {
            "attributesToRetrieve": self.attributes_to_retrieve,
            "attributesToHighlight": [],
            "offset": offset,
            "length": limit
        })

        return [dict([(attr, hit[attr]) for attr in self.attributes_to_retrieve]) for hit in result["hits"]]


class SearchTickers(SearchAction):

    def __init__(self, algolia_app_id, algolia_search_api_key, tickers_index):
        super().__init__("search_tickers", algolia_app_id, algolia_search_api_key, tickers_index, ["symbol"])


class SearchCollections(SearchAction):

    def __init__(self, algolia_app_id, algolia_search_api_key, tickers_index):
        super().__init__("search_collections", algolia_app_id, algolia_search_api_key, tickers_index, ["collection_id"])
