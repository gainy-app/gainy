from typing import List, Iterable

from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from common.hasura_response import format_datetime
from exceptions import EntityNotFoundException
from gainy.trading.models import TradingCollectionVersion
from gainy.utils import get_logger

logger = get_logger(__name__)


class TradingGetCollectionData(HasuraAction):

    def __init__(self):
        super().__init__("trading_get_collection_data", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params['profile_id']
        collection_id = input_params['collection_id']

        trading_service = context_container.trading_service
        trading_repository = context_container.trading_repository

        try:
            actual_value = trading_service.get_actual_collection_status(
                profile_id, collection_id).value
        except EntityNotFoundException:
            actual_value = 0

        trading_collection_versions: List[
            TradingCollectionVersion] = trading_repository.find_all(
                TradingCollectionVersion, {
                    "profile_id": profile_id,
                    "collection_id": collection_id
                }, [("created_at", "DESC")])

        success_tcvs = filter(lambda x: x.is_executed(), trading_collection_versions)
        pending_tcvs = filter(lambda x: x.is_pending(), trading_collection_versions)

        return {
            'actual_value': actual_value,
            'history': {
                "pending": self._format_tcvs(pending_tcvs),
                "successful": self._format_tcvs(success_tcvs),
            }
        }

    def _format_tcvs(self, tcvs: Iterable[TradingCollectionVersion]):
        return [{
            "target_amount_delta": i.target_amount_delta,
            "created_at": format_datetime(i.created_at),
            "executed_at": format_datetime(i.executed_at),
        } for i in tcvs]
