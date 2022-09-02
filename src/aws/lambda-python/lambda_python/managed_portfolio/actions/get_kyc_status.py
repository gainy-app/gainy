from common.context_container import ContextContainer
from common.exceptions import ApiException
from common.hasura_exception import HasuraActionException
from common.hasura_function import HasuraAction
from managed_portfolio import ManagedPortfolioService, ManagedPortfolioRepository
from psycopg2.extras import RealDictCursor
from gainy.utils import get_logger

logger = get_logger(__name__)


class GetKycStatus(HasuraAction):

    def __init__(self, action_name="get_kyc_status"):
        super().__init__(action_name, "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        managed_portfolio_service = context_container.managed_portfolio_service
        db_conn = context_container.db_conn
        profile_id = input_params["profile_id"]

        # TODO make async (with SQS)

        repository = context_container.managed_portfolio_repository

        try:
            kyc_status = managed_portfolio_service.get_kyc_status(profile_id)
            repository.update_kyc_form(profile_id, kyc_status.status)
            error_message = None
        except ApiException as e:
            error_message = str(e)

        return {"error_message": error_message, "status": kyc_status.status}
