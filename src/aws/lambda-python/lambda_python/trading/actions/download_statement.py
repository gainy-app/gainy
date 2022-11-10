from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from exceptions import ForbiddenException
from gainy.exceptions import NotFoundException
from gainy.utils import get_logger
from trading.models import TradingStatement

logger = get_logger(__name__)


class TradingDownloadDocument(HasuraAction):

    def __init__(self, action_name="trading_download_statement"):
        super().__init__(action_name, "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params["profile_id"]
        statement_id = input_params["statement_id"]

        statement: TradingStatement = context_container.trading_repository.find_one(
            TradingStatement, {"id": statement_id})
        if not statement:
            raise NotFoundException
        if statement.profile_id != profile_id:
            raise ForbiddenException

        trading_service = context_container.trading_service
        url = trading_service.download_statement(statement)

        return {"url": url}
