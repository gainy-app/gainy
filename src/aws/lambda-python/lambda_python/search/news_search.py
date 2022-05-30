import json
import time
from datetime import datetime
import pytz
import backoff
import requests
from backoff import full_jitter
import traceback
from common.hasura_function import HasuraAction
from search.cache import CachingLoader, RedisCache
from services.logging import get_logger

logger = get_logger(__name__)


@backoff.on_predicate(backoff.expo,
                      predicate=lambda res: res.status_code == 429,
                      max_tries=3,
                      jitter=lambda v: v / 2 + full_jitter(v / 2))
def http_get_request(url: str):
    return requests.get(url)


def load_url(url: str):
    response = http_get_request(url)
    if response.status_code == 200:
        return response.content
    else:
        raise Exception(
            f"Url not loaded with status: {response.status_code}, response: {response.content}"
        )


class SearchNews(HasuraAction):

    def __init__(self, gnews_api_token, redis_host, redis_port):
        super().__init__("fetchNewsData")
        self.gnews_api_token = gnews_api_token
        self.caching_loader = CachingLoader(
            RedisCache(redis_host, redis_port, ttl_seconds=60 * 60), load_url)

    def apply(self, db_conn, input_params, headers):
        query = input_params["symbol"]
        limit = input_params.get("limit", 5)

        url = self._build_url(query, limit)
        try:
            response_json = json.loads(self.caching_loader.get(url))

            articles = response_json["articles"]

            return [{
                "datetime": self._reformat_datetime(article["publishedAt"]),
                "title": article["title"],
                "description": article["description"],
                "url": article["url"],
                "imageUrl": article["image"],
                "sourceName": article["source"]["name"],
                "sourceUrl": article["source"]["url"]
            } for article in articles]
        except:
            logger.error(
                "Exception thrown in SearchNews: %s, url: %s, input_params: %s",
                json.dumps(traceback.format_exception()), url,
                json.dumps(input_params))

            return []

    @staticmethod
    def _reformat_datetime(time_string):
        parsed_datetime = pytz.utc.localize(
            datetime.strptime(time_string, "%Y-%m-%dT%H:%M:%SZ"))
        return datetime.strftime(parsed_datetime, "%Y-%m-%dT%H:%M:%S%z")

    def _build_url(self, query, limit) -> str:
        return f"https://gnews.io/api/v4/search?token={self.gnews_api_token}&q=\"{query}\"&max={limit}&lang=en"
