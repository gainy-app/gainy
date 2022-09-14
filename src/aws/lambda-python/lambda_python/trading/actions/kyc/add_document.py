from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from trading.models import KycDocument
from gainy.utils import get_logger

logger = get_logger(__name__)


class KycAddDocument(HasuraAction):

    def __init__(self, action_name="kyc_add_document"):
        super().__init__(action_name, "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        trading_service = context_container.trading_service
        profile_id = input_params["profile_id"]

        model = KycDocument()
        model.uploaded_file_id = input_params["uploaded_file_id"]
        model.type = input_params["type"]
        model.side = input_params["side"]

        trading_service.send_kyc_document(profile_id, model)

        return {"ok": True}
