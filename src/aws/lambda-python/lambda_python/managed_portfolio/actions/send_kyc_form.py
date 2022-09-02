from common.context_container import ContextContainer
from common.exceptions import ApiException
from common.hasura_exception import HasuraActionException
from common.hasura_function import HasuraAction
from managed_portfolio import ManagedPortfolioService, ManagedPortfolioRepository
from psycopg2.extras import RealDictCursor
from gainy.utils import get_logger

logger = get_logger(__name__)


class SendKycForm(HasuraAction):

    def __init__(self, action_name="send_kyc_form"):
        super().__init__(action_name, "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        managed_portfolio_service = context_container.managed_portfolio_service
        db_conn = context_container.db_conn
        profile_id = input_params["profile_id"]

        repository = context_container.managed_portfolio_repository
        kyc_form = repository.get_kyc_form(profile_id)

        try:
            kyc_status = managed_portfolio_service.send_kyc_form(kyc_form)
            error_message = None
        except ApiException as e:
            error_message = str(e)

        repository.update_kyc_form(profile_id, kyc_status.status)
        return {"error_message": error_message, "status": kyc_status.status}
