import datetime
import os

from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.exceptions import BadRequestException
from gainy.utils import get_logger
from services.cache import Cache
from services.twilio import TwilioClient
from verification.exceptions import CooldownException

logger = get_logger(__name__)

APP_STORE_LINK = os.getenv("APP_STORE_LINK", "https://go.gainy.app/ZOFw")
APP_STORE_LINK_COOLDOWN = 30


def _validate_phone_number(twilio_client: TwilioClient, phone_number: str):
    twilio_client.validate_phone_number(phone_number)


def _send(twilio_client: TwilioClient, phone_number):
    if not twilio_client.send_sms(phone_number, APP_STORE_LINK):
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
        user_id = context_container.request["session_variables"][
            "x-hasura-user-id"]
        phone_number = input_params["phone_number"]

        cache = context_container.cache
        twilio_client = context_container.twilio_client

        try:
            _check_can_send(cache, user_id)
            _validate_phone_number(twilio_client, phone_number)
            _send(twilio_client, phone_number)
            _mark_sent(cache, user_id)
        except CooldownException:
            raise BadRequestException(
                'You have requested a link too soon from the previous one.')

        return {"ok": True}
