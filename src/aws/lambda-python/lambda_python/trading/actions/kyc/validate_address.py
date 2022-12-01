from common.context_container import ContextContainer
from gainy.exceptions import NotFoundException
from common.hasura_function import HasuraAction
from gainy.utils import get_logger, DATETIME_ISO8601_FORMAT_TZ
from trading.kyc_form_validator import KycFormValidator

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

        result = KycFormValidator.validate_address(
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
                "street1": result.get('street1'),
                "street2": result.get('street2'),
                "city": result.get('city'),
                "province": result.get('province'),
                "postal_code": result.get('postal_code'),
                "country": result.get('country'),
            }
        }
