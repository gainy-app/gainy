from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.utils import get_logger

logger = get_logger(__name__)


class TradingSyncProviderData(HasuraAction):

    def __init__(self):
        super().__init__("trading_sync_provider_data", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params["profile_id"]

        trading_service = context_container.trading_service
        trading_service.sync_provider_data(profile_id)

        return {"ok": True}
