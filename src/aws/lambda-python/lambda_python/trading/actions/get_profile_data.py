from typing import List, Iterable

from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from common.hasura_response import format_datetime
from exceptions import EntityNotFoundException
from gainy.utils import get_logger
from trading.models import TradingMoneyFlow, TradingMoneyFlowStatus, ProfileBalances

logger = get_logger(__name__)


class TradingGetProfileData(HasuraAction):

    def __init__(self):
        super().__init__("trading_get_profile_data", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params['profile_id']

        trading_service = context_container.trading_service
        trading_repository = context_container.trading_repository

        try:
            balances = trading_service.get_actual_balances(profile_id)
        except EntityNotFoundException:
            balances = ProfileBalances()

        pending_money_flows: List[
            TradingMoneyFlow] = trading_repository.find_all(
                TradingMoneyFlow, {
                    "profile_id": profile_id,
                    "status": TradingMoneyFlowStatus.PENDING.name
                }, [("created_at", "DESC")])

        return {
            'withdrawable_cash': balances.withdrawable_cash or 0,
            'buying_power': balances.buying_power or 0,
            'history': {
                "pending": self._format_mfs(pending_money_flows),
            }
        }

    def _format_mfs(self, mfs: Iterable[TradingMoneyFlow]):
        return [{
            "amount": i.amount,
            "created_at": format_datetime(i.created_at),
        } for i in mfs]
