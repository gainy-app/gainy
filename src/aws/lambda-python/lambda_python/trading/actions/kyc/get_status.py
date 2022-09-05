from common.context_container import ContextContainer
from common.exceptions import ApiException
from common.hasura_exception import HasuraActionException
from common.hasura_function import HasuraAction
from trading import TradingService, TradingRepository
from psycopg2.extras import RealDictCursor
from gainy.utils import get_logger

logger = get_logger(__name__)


class KycGetStatus(HasuraAction):

    def __init__(self, action_name="kyc_get_status"):
        super().__init__(action_name, "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        trading_service = context_container.trading_service
        db_conn = context_container.db_conn
        profile_id = input_params["profile_id"]

        # TODO make async (with SQS)

        repository = context_container.trading_repository

        try:
            kyc_status = trading_service.kyc_get_status(profile_id)
            repository.update_kyc_form(profile_id, kyc_status.status)
            error_message = None
        except ApiException as e:
            error_message = str(e)

        return {"error_message": error_message, "status": kyc_status.status}
