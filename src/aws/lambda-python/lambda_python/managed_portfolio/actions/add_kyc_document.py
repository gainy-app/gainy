import logging
from common.context_container import ContextContainer
from common.exceptions import ApiException
from common.hasura_exception import HasuraActionException
from common.hasura_function import HasuraAction
from managed_portfolio import ManagedPortfolioService, ManagedPortfolioRepository
from managed_portfolio.models import KycDocument
from psycopg2.extras import RealDictCursor

logger = logging.getLogger()


class AddKycDocument(HasuraAction):

    def __init__(self, action_name="add_kyc_document"):
        super().__init__(action_name, "profile_id")
        self.service = ManagedPortfolioService()

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params["profile_id"]

        model = KycDocument(input_params["uploaded_file_id"],
                            input_params["type"], input_params["side"])

        try:
            kyc_status = self.service.send_kyc_document(
                context_container, profile_id, model)
            error_message = None
        except ApiException as e:
            error_message = str(e)


#         repository.update_kyc_form(profile_id, kyc_status.status)
        return {"error_message": error_message}
