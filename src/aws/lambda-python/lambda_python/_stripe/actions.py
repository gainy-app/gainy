import json

from _stripe.api import STRIPE_PUBLISHABLE_KEY
from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from common.hasura_exception import HasuraActionException
from gainy.billing.models import PaymentMethod, PaymentMethodProvider
from gainy.billing.stripe.models import StripePaymentMethod, StripePaymentIntent
from gainy.utils import get_logger
from models import Profile

logger = get_logger(__name__)


def handle_error(e):
    logger.exception('Stripe Error: %s' % (e))

    raise HasuraActionException(400, "Stripe error: %s" % (e))


class StripeGetCheckoutUrl(HasuraAction):

    def __init__(self):
        super().__init__("stripe_get_checkout_url")

    def apply(self, input_params, context_container: ContextContainer):
        price_id = input_params["price_id"]
        to_refund = input_params.get("to_refund", True)
        mode = input_params.get("mode", "payment")
        success_url = input_params["success_url"]
        cancel_url = input_params["cancel_url"]

        api = context_container.stripe_api
        repository = context_container.get_repository()

        try:
            checkout_session = api.create_checkout_session(
                price_id, mode, success_url, cancel_url)
            payment_intent_id = checkout_session.payment_intent
            logger.info('Created Stripe session %s with payment intent %s' %
                        (checkout_session.id, payment_intent_id))

            payment_intent = StripePaymentIntent()
            payment_intent.ref_id = payment_intent_id
            payment_intent.to_refund = to_refund
            repository.persist(payment_intent)

            return {'url': checkout_session.url}
        except Exception as e:
            handle_error(e)


class StripeGetPaymentSheetData(HasuraAction):

    def __init__(self):
        super().__init__("stripe_get_payment_sheet_data")

    def apply(self, input_params, context_container: ContextContainer):
        api = context_container.stripe_api
        repository = context_container.get_repository()

        profile_id = input_params["profile_id"]
        profile = repository.find_one(Profile, {"id": profile_id})

        customer_id = api.upsert_customer(profile).id

        ephemeral_key = api.create_ephemeral_key(customer_id)
        setup_intent = api.create_setup_intent(customer_id)

        return {
            "setup_intent_client_secret": setup_intent.client_secret,
            "ephemeral_key": ephemeral_key.secret,
            "customer_id": customer_id,
            "publishable_key": STRIPE_PUBLISHABLE_KEY
        }


class StripeWebhook(HasuraAction):

    def __init__(self):
        super().__init__("stripe_webhook")

    def apply(self, input_params, context_container: ContextContainer):
        event_id = input_params['id']
        event = context_container.stripe_api.retrieve_event(event_id)

        logger.info('[STRIPE_WEBHOOK] event', extra={"event": event})

        if event.type == 'payment_intent.succeeded':
            self._handle_payment_intent_succeeded(context_container,
                                                  event.data.object)

        elif event.type in [
                "payment_method.updated",
                "payment_method.automatically_updated",
                "payment_method.attached"
        ]:
            self._upsert_payment_method(context_container, event.data.object)

        elif event.type == "payment_method.detached":
            self._delete_payment_method(context_container, event.data.object)

        else:
            raise Exception("Unsupported event %s", event.type)

        return {"ok": True}

    def _handle_payment_intent_succeeded(self,
                                         context_container: ContextContainer,
                                         data):
        api = context_container.stripe_api
        repository = context_container.get_repository()
        payment_intent_id = data['id']

        payment_intent = repository.find_one(StripePaymentIntent,
                                             {"ref_id": payment_intent_id})
        if not payment_intent:
            payment_intent = StripePaymentIntent()
        payment_intent.set_from_response(data)
        repository.persist(payment_intent)

        if not payment_intent.to_refund:
            return

        try:
            refund = api.create_refund(payment_intent_id)
            logger.info('[STRIPE_WEBHOOK] refund %s', json.dumps(refund))

            payment_intent.is_refunded = True
            payment_intent.refund_data = refund
            repository.persist(payment_intent)
        except Exception as e:
            logger.info('[STRIPE_WEBHOOK] error while refund %s', e)
            handle_error(e)

    def _upsert_payment_method(self, context_container: ContextContainer,
                               data):
        api = context_container.stripe_api
        repository = context_container.get_repository()

        stripe_payment_method = repository.find_one(StripePaymentMethod,
                                                    {"ref_id": data["id"]})
        if not stripe_payment_method:
            stripe_payment_method = StripePaymentMethod()
        stripe_payment_method.set_from_response(data)
        repository.persist(stripe_payment_method)

        if not stripe_payment_method.payment_method_id:
            customer = api.retrieve_customer(data["customer"])
            if not customer or "profile_id" not in customer.metadata:
                return

            profile_id = customer.metadata["profile_id"]

            payment_method = PaymentMethod()
            payment_method.provider = PaymentMethodProvider.STRIPE
            payment_method.profile_id = profile_id
            payment_method.name = stripe_payment_method.name
            repository.persist(payment_method)
            stripe_payment_method.payment_method_id = payment_method.id
            repository.persist(stripe_payment_method)

    def _delete_payment_method(self, context_container: ContextContainer,
                               data):
        repository = context_container.get_repository()

        stripe_payment_method = repository.find_one(StripePaymentMethod,
                                                    {"ref_id": data["id"]})
        if not stripe_payment_method:
            return

        if stripe_payment_method.payment_method_id:
            payment_method = repository.find_one(
                PaymentMethod, {"id": stripe_payment_method.payment_method_id})
            repository.delete(payment_method)

        repository.delete(stripe_payment_method)
