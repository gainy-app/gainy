import json
from datetime import datetime
import dateutil.parser
import backoff
import re
import requests
from urllib.parse import urlencode
from backoff import full_jitter
from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from search.cache import CachingLoader, RedisCache
from gainy.utils import get_logger, DATETIME_ISO8601_FORMAT_TZ

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

    def apply(self, input_params, context_container: ContextContainer):
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
                "Exception thrown in SearchNews: url: %s, input_params: %s",
                url, json.dumps(input_params))

            return []

    @staticmethod
    def _reformat_datetime(time_string):
        return dateutil.parser.parse(time_string).strftime(
            DATETIME_ISO8601_FORMAT_TZ)

    def _build_url(self, query, limit) -> str:
        query = re.sub(r'\.CC$', '', query)
        query = re.sub(r'\.INDX$', '', query)

        url = "https://gnews.io/api/v4/search?"
        params = {
            "token": self.gnews_api_token,
            "q": '"%s"' % (query),
            "max": limit,
            "lang": "en"
        }
        return url + urlencode(params)
