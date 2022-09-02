from common.context_container import ContextContainer
from common.exceptions import ApiException
from common.hasura_exception import HasuraActionException
from common.hasura_function import HasuraAction
from managed_portfolio import ManagedPortfolioService, ManagedPortfolioRepository
from managed_portfolio.models import KycDocument
from psycopg2.extras import RealDictCursor
from gainy.utils import get_logger

logger = get_logger(__name__)


class AddKycDocument(HasuraAction):

    def __init__(self, action_name="add_kyc_document"):
        super().__init__(action_name, "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        managed_portfolio_service = context_container.managed_portfolio_service
        profile_id = input_params["profile_id"]

        model = KycDocument(input_params["uploaded_file_id"],
                            input_params["type"], input_params["side"])

        try:
            kyc_status = managed_portfolio_service.send_kyc_document(
                context_container, profile_id, model)
            error_message = None
        except ApiException as e:
            error_message = str(e)

        return {"error_message": error_message}
