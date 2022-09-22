import json
import os
from typing import Optional

import stripe
from stripe.api_resources.checkout.session import Session
from stripe.api_resources.customer import Customer
from stripe.api_resources.ephemeral_key import EphemeralKey
from stripe.api_resources.event import Event
from stripe.api_resources.payment_method import PaymentMethod
from stripe.api_resources.refund import Refund
from stripe.api_resources.setup_intent import SetupIntent
from gainy.billing.stripe.api import StripeApi as GainyStripeApi
from gainy.utils import get_logger

from models import Profile

logger = get_logger(__name__)

STRIPE_PUBLISHABLE_KEY = os.getenv('STRIPE_PUBLISHABLE_KEY')


class StripeApi(GainyStripeApi):

    def detach_payment_method(self, payment_method_id) -> PaymentMethod:
        return stripe.PaymentMethod.detach(payment_method_id)

    def create_checkout_session(self, price_id, mode, success_url,
                                cancel_url) -> Session:
        return stripe.checkout.Session.create(
            line_items=[
                {
                    'price': price_id,
                    'quantity': 1,
                },
            ],
            mode=mode,
            success_url=success_url,
            cancel_url=cancel_url,
        )

    def create_ephemeral_key(self, customer_id) -> EphemeralKey:
        return stripe.EphemeralKey.create(
            customer=customer_id,
            stripe_version='2020-08-27',
        )

    def create_setup_intent(self, customer_id) -> SetupIntent:
        return stripe.SetupIntent.create(customer=customer_id, )

    def create_customer(self, profile: Profile) -> Customer:
        return stripe.Customer.create(
            email=profile.email,
            name=' '.join([profile.first_name, profile.last_name]),
            metadata={"profile_id": profile.id})

    def find_customer(self, profile) -> Optional[Customer]:
        customers = stripe.Customer.search(
            query=f"metadata['profile_id']:'{profile.id}'").data
        logger.info('find_customer',
                    extra={"customers": json.dumps(customers)})

        if customers:
            return customers[0]

        return None

    def upsert_customer(self, profile: Profile):
        return self.find_customer(profile) or self.create_customer(profile)

    def retrieve_event(self, event_id) -> Event:
        return stripe.Event.retrieve(event_id)

    def retrieve_customer(self, customer_id) -> Optional[Customer]:
        return stripe.Customer.retrieve(customer_id)

    def create_refund(self, payment_intent_id) -> Refund:
        return stripe.Refund.create(payment_intent=payment_intent_id)
