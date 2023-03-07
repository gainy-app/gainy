from common.context_container import ContextContainer
from common.hasura_function import HasuraTrigger
from gainy.utils import get_logger
from trading.models import ProfileKycStatus

logger = get_logger(__name__)


class OnNewKycStatus(HasuraTrigger):

    def __init__(self):
        super().__init__("on_new_kyc_status")

    def get_allowed_profile_ids(self, op, data):
        payload = self._extract_payload(data)
        return [payload['profile_id']]

    def apply(self, op, data, context_container: ContextContainer):
        payload = self._extract_payload(data)
        status_id = payload['id']
        status = context_container.get_repository().find_one(
            ProfileKycStatus, {"id": status_id})
        if not status:
            return
        context_container.trading_service.handle_kyc_status_change(status)
