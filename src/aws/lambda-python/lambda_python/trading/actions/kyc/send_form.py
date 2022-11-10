from common.context_container import ContextContainer
from gainy.exceptions import NotFoundException
from common.hasura_function import HasuraAction
from gainy.utils import get_logger, DATETIME_ISO8601_FORMAT_TZ

logger = get_logger(__name__)


class KycSendForm(HasuraAction):

    def __init__(self, action_name="kyc_send_form"):
        super().__init__(action_name, "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        trading_service = context_container.trading_service
        profile_id = input_params["profile_id"]

        repository = context_container.trading_repository
        kyc_form = repository.get_kyc_form(profile_id)

        if not kyc_form:
            raise NotFoundException()

        trading_service.validate_kyc_form(kyc_form)
        kyc_status = trading_service.kyc_send_form(kyc_form)
        repository.update_kyc_form(profile_id, kyc_status.status)
        return {
            "status":
            kyc_status.status,
            "message":
            kyc_status.message,
            "updated_at":
            kyc_status.updated_at.strftime(DATETIME_ISO8601_FORMAT_TZ),
        }
