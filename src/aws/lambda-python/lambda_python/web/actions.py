import os
import json

import stripe

from common.hasura_function import HasuraAction
from common.hasura_exception import HasuraActionException
from gainy.utils import get_logger

logger = get_logger(__name__)

stripe.api_key = os.getenv('STRIPE_API_KEY')


def handle_error(e):
    logger.exception('Stripe Error: %s' % (e))

    raise HasuraActionException(400, "Stripe error: %s" % (e))


class StripeGetCheckoutUrl(HasuraAction):

    def __init__(self):
        super().__init__("stripe_get_checkout_url")

    def apply(self, db_conn, input_params, headers):
        price_id = input_params["price_id"]
        success_url = input_params["success_url"]
        cancel_url = input_params["cancel_url"]

        try:
            checkout_session = stripe.checkout.Session.create(
                line_items=[
                    {
                        'price': price_id,
                        'quantity': 1,
                    },
                ],
                mode='payment',
                success_url=success_url,
                cancel_url=cancel_url,
            )
        except Exception as e:
            handle_error(e)

        logger.info('Created Stripe session %s with payment intent %s' %
                    (checkout_session.id, checkout_session.payment_intent))

        return {'url': checkout_session.url}


class StripeWebhook(HasuraAction):

    def __init__(self):
        super().__init__("stripe_webhook")

    def apply(self, db_conn, input_params, headers):
        logger.info("[STRIPE_WEBHOOK] event pre %s", input_params)

        event_id = input_params['id']
        event = stripe.Event.retrieve(event_id)

        logger.info('[STRIPE_WEBHOOK] event post %s', json.dumps(event))

        if event.type == 'payment_intent.succeeded':
            payment_intent = event.data.object
            try:
                refund = stripe.Refund.create(
                    payment_intent=payment_intent['id'])
                logger.info('[STRIPE_WEBHOOK] refund %s', json.dumps(refund))
            except Exception as e:
                logger.info('[STRIPE_WEBHOOK] error while refund %s', e)
                handle_error(e)

        return {"success": True}
