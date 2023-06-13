from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.utils import get_logger

logger = get_logger(__name__)


class KycValidateAddress(HasuraAction):

    def __init__(self, action_name="kyc_validate_address"):
        super().__init__(action_name)

    def apply(self, input_params, context_container: ContextContainer):
        fields = [
            'street1',
            'street2',
            'city',
            'province',
            'postal_code',
            'country',
        ]

        address = {}
        for f in fields:
            address[f] = input_params.get(f)

        address = context_container.kyc_form_validator.validate_address(
            street1=address['street1'],
            street2=address['street2'],
            city=address['city'],
            province=address['province'],
            postal_code=address['postal_code'],
            country=address['country'],
        )

        return {
            "ok": True,
            "suggested": {
                "street1": address.street1,
                "street2": None,
                "city": address.city,
                "province": address.province,
                "postal_code": address.postal_code,
                "country": address.country,
            }
        }
