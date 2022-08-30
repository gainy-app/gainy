import logging
from common.context_container import ContextContainer
from common.exceptions import ApiException
from common.hasura_exception import HasuraActionException
from common.hasura_function import HasuraAction
from managed_portfolio import ManagedPortfolioService, ManagedPortfolioRepository
from psycopg2.extras import RealDictCursor

logger = logging.getLogger()


class GetKycStatus(HasuraAction):

    def __init__(self, action_name="get_kyc_status"):
        super().__init__(action_name, "profile_id")
        self.service = ManagedPortfolioService()

    def apply(self, input_params, context_container: ContextContainer):
        db_conn = context_container.db_conn
        profile_id = input_params["profile_id"]

        # TODO make async (with SQS)

        repository = ManagedPortfolioRepository(context_container)

        try:
            kyc_status = self.service.get_kyc_status(context_container,
                                                     profile_id)
            error_message = None
        except ApiException as e:
            error_message = str(e)

        repository.update_kyc_form(profile_id, kyc_status.status)

        return {"error_message": error_message, "status": kyc_status.status}
