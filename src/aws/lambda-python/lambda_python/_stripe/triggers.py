from common.context_container import ContextContainer
from common.hasura_function import HasuraTrigger
from common.hasura_exception import HasuraActionException
from gainy.utils import get_logger

logger = get_logger(__name__)


def handle_error(e):
    logger.exception('Stripe Error: %s' % (e))

    raise HasuraActionException(400, "Stripe error: %s" % (e))


class StripeDeletePaymentMethod(HasuraTrigger):

    def __init__(self):
        super().__init__("stripe_payment_method_deleted")

    def get_allowed_profile_ids(self, op, data):
        return None

    def apply(self, op, data, context_container: ContextContainer):
        stripe_api = context_container.stripe_api
        payload = self._extract_payload(data)
        payment_method_id = payload['ref_id']

        stripe_api.detach_payment_method(payment_method_id)
