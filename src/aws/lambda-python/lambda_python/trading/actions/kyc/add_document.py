from common.context_container import ContextContainer
from common.exceptions import ApiException
from common.hasura_exception import HasuraActionException
from common.hasura_function import HasuraAction
from trading import TradingService, TradingRepository
from trading.models import KycDocument
from psycopg2.extras import RealDictCursor
from gainy.utils import get_logger

logger = get_logger(__name__)


class KycAddDocument(HasuraAction):

    def __init__(self, action_name="kyc_add_document"):
        super().__init__(action_name, "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        trading_service = context_container.trading_service
        profile_id = input_params["profile_id"]

        model = KycDocument(input_params["uploaded_file_id"],
                            input_params["type"], input_params["side"])

        trading_service.send_kyc_document(context_container, profile_id, model)

        return {"ok": True}
