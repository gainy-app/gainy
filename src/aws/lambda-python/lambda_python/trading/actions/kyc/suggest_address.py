from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.utils import get_logger
from trading.kyc_form_validator import ALLOWED_COUNTRY_CODES

logger = get_logger(__name__)


class KycSuggestAddresses(HasuraAction):

    def __init__(self, action_name="kyc_suggest_addresses"):
        super().__init__(action_name)

    def apply(self, input_params, context_container: ContextContainer):
        query = input_params["query"]
        limit = input_params["limit"]

        google_maps_service = context_container.google_maps_service

        result = google_maps_service.suggest_addresses(
            query, limit=limit, allowed_country_codes=ALLOWED_COUNTRY_CODES)

        return [{
            "formatted_address": i.formatted_address,
            "street1": i.street1,
            "city": i.city,
            "province": i.province,
            "postal_code": i.postal_code,
            "country": i.country,
        } for i in result]
