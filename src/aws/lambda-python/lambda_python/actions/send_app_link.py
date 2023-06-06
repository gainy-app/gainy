import datetime
import os
import requests

from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.exceptions import BadRequestException
from gainy.utils import get_logger
from services.cache import Cache
from services.twilio import TwilioClient
from verification.exceptions import CooldownException

logger = get_logger(__name__)

REBRANDLY_API_KEY = os.getenv("REBRANDLY_API_KEY")
APP_STORE_LINK = os.getenv("APP_STORE_LINK", "https://go.gainy.app/ZOFw")
APP_STORE_LINK_COOLDOWN = 30


def _validate_phone_number(twilio_client: TwilioClient, phone_number: str):
    twilio_client.validate_phone_number(phone_number)


def shorten_url(url):
    payload = {"destination": url}
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "apikey": REBRANDLY_API_KEY
    }
    response = requests.post("https://api.rebrandly.com/v1/links",
                             headers=headers,
                             json=payload)
    return "https://" + response.json()["shortUrl"]


def _send(twilio_client: TwilioClient, phone_number, query_string):
    url = APP_STORE_LINK
    if query_string:
        url += f"?{query_string}"
        try:
            url = shorten_url(url)
        except Exception as e:
            logger.exception(e)

    if not twilio_client.send_sms(phone_number, f"Download Gainy app: {url}"):
        raise Exception('Failed to send link.')


def _get_cache_key(user_id):
    return f"app_link_last_sent_{user_id}"


def _check_can_send(cache: Cache, user_id):
    last_sent = cache.get(_get_cache_key(user_id))
    if last_sent is None:
        return

    threshold_timestamp = (
        datetime.datetime.now() -
        datetime.timedelta(seconds=int(APP_STORE_LINK_COOLDOWN))).timestamp()

    if last_sent < threshold_timestamp:
        return
    raise CooldownException


def _mark_sent(cache: Cache, user_id):
    cache.set(_get_cache_key(user_id),
              datetime.datetime.now().timestamp(),
              int(APP_STORE_LINK_COOLDOWN))


# TODO tests
class SendAppLink(HasuraAction):

    def __init__(self, action_name="send_app_link"):
        super().__init__(action_name)

    def apply(self, input_params, context_container: ContextContainer):
        user_id = context_container.request["session_variables"].get(
            "x-hasura-user-id")
        phone_number = input_params["phone_number"]
        query_string = input_params.get("query_string")

        cache = context_container.cache
        twilio_client = context_container.twilio_client

        try:
            if user_id is not None:
                _check_can_send(cache, user_id)
            _validate_phone_number(twilio_client, phone_number)
            _send(twilio_client, phone_number, query_string)
            if user_id is not None:
                _mark_sent(cache, user_id)
        except CooldownException:
            raise BadRequestException(
                'You have requested a link too soon from the previous one.')

        return {"ok": True}
