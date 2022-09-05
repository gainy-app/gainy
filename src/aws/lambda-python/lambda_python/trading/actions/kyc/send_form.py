from common.context_container import ContextContainer
from common.exceptions import ApiException, NotFoundException
from common.hasura_exception import HasuraActionException
from common.hasura_function import HasuraAction
from trading import TradingService, TradingRepository
from psycopg2.extras import RealDictCursor
from gainy.utils import get_logger

logger = get_logger(__name__)


class KycSendForm(HasuraAction):

    def __init__(self, action_name="kyc_send_form"):
        super().__init__(action_name, "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        trading_service = context_container.trading_service
        db_conn = context_container.db_conn
        profile_id = input_params["profile_id"]

        repository = context_container.trading_repository
        kyc_form = repository.get_kyc_form(profile_id)

        if not kyc_form:
            raise NotFoundException()

        try:
            kyc_status = trading_service.kyc_send_form(kyc_form)
            error_message = None
        except ApiException as e:
            error_message = str(e)

        repository.update_kyc_form(profile_id, kyc_status.status)
        return {"error_message": error_message, "status": kyc_status.status}
