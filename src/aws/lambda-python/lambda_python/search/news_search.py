import json
import dateutil.parser
import backoff
import re
import requests
from urllib.parse import urlencode
from backoff import full_jitter
from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from services.cache import CachingLoader, RedisCache
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
        return response.text
    else:
        raise Exception(
            f"Url not loaded with status: {response.status_code}, response: {response.text}"
        )


class SearchNews(HasuraAction):

    def __init__(self, gnews_api_token):
        super().__init__("fetchNewsData")
        self.gnews_api_token = gnews_api_token

    def apply(self, input_params, context_container: ContextContainer):
        query = input_params["symbol"]
        limit = input_params.get("limit", 5)
        caching_loader = CachingLoader(context_container.cache,
                                       load_url,
                                       ttl_seconds=60 * 60)

        url = self._build_url(query, limit)
        logger_extra = {
            "input_params": input_params,
            "limit": limit,
            "url": url
        }

        try:
            data = caching_loader.get(url)
            logger_extra["data"] = data
            response_json = json.loads(data)

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
        except Exception as e:
            logger.exception("SearchNews: %s", e, extra=logger_extra)

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
