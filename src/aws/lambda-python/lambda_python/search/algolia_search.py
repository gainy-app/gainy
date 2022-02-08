from abc import ABC, abstractmethod
from typing import List, Any

from algoliasearch.search_client import SearchClient

from common.hasura_function import HasuraAction
from operator import itemgetter


class SearchAction(HasuraAction, ABC):

    def __init__(self, action_name, algolia_app_id, algolia_search_api_key,
                 tickers_index, attributes_to_retrieve, key_attribute):
        super().__init__(action_name)
        search_client = SearchClient.create(algolia_app_id,
                                            algolia_search_api_key)
        self.search_index = search_client.init_index(tickers_index)
        self.attributes_to_retrieve = attributes_to_retrieve
        self.key_attribute = key_attribute

    def apply(self, db_conn, input_params, headers):
        query = input_params["query"]
        offset = input_params.get("offset", 0)
        limit = input_params.get("limit", 10)

        result = self.search_index.search(
            query, {
                "attributesToRetrieve": self.attributes_to_retrieve,
                "attributesToHighlight": [],
                "offset": offset,
                "length": limit
            })

        record_list = [
            dict([(attr, hit[attr]) for attr in self.attributes_to_retrieve])
            for hit in result["hits"]
        ]

        key_list = [record[self.key_attribute] for record in record_list]
        key_flags = self.check_if_exists(db_conn, key_list)

        return [
            record_and_flag[0]
            for record_and_flag in zip(record_list, key_flags)
            if record_and_flag[1]
        ]

    @abstractmethod
    def _enabled_only(self) -> bool:
        pass

    def check_if_exists(self, db_conn, key_list: List[Any]) -> List[bool]:
        if not key_list:
            return []

        with db_conn.cursor() as cursor:
            statement = f"SELECT {self.key_attribute} FROM {self.table_name()} WHERE {self.key_attribute} IN %(key_list)s"
            if self._enabled_only():
                statement += " AND enabled = '1'"

            cursor.execute(statement, {"key_list": tuple(key_list)})

            existing_keys = set(map(itemgetter(0), cursor.fetchall()))

            return [key in existing_keys for key in key_list]

    @abstractmethod
    def table_name(self) -> str:
        pass


class SearchTickers(SearchAction):

    def __init__(self, algolia_app_id, algolia_search_api_key, tickers_index):
        super().__init__("search_tickers", algolia_app_id,
                         algolia_search_api_key, tickers_index, ["symbol"],
                         "symbol")

    def table_name(self) -> str:
        return "public.tickers"

    def _enabled_only(self) -> bool:
        return False


class SearchCollections(SearchAction):

    def __init__(self, algolia_app_id, algolia_search_api_key, tickers_index):
        super().__init__("search_collections", algolia_app_id,
                         algolia_search_api_key, tickers_index, ["id"], "id")

    def table_name(self) -> str:
        return "public.collections"

    def _enabled_only(self) -> bool:
        return True
