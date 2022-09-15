import json

import stripe

from gainy.data_access.models import BaseModel, classproperty


class StripePaymentMethod(BaseModel):
    ref_id = None
    payment_method_id = None
    name = None
    data = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    def update(self, data):
        self.ref_id = data["id"]
        self.data = stripe.util.convert_to_dict(data)
        self.name = self._get_payment_method_name()

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "data": json.dumps(self.data),
        }

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "stripe_payment_methods"

    def _get_payment_method_name(self):
        pieces = [self.data["type"]]

        params = self.data[self.data["type"]]
        for field in [
                "bank_name", "bank", "bsb_number", "brand", "tax_id", "last4",
                "email"
        ]:
            if field not in params:
                continue
            pieces.append(params[field])

        return ' '.join(pieces)
