from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.utils import get_logger, DATETIME_ISO8601_FORMAT_TZ

logger = get_logger(__name__)


class KycGetStatus(HasuraAction):

    def __init__(self, action_name="kyc_get_status"):
        super().__init__(action_name, "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params["profile_id"]

        repository = context_container.trading_repository
        kyc_status = repository.get_actual_kyc_status(profile_id)

        return {
            "status":
            kyc_status.status,
            "message":
            kyc_status.message,
            "updated_at":
            kyc_status.created_at.strftime(DATETIME_ISO8601_FORMAT_TZ)
        }
