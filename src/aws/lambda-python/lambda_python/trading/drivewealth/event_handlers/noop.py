from gainy.utils import get_logger
from trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler

logger = get_logger(__name__)


class NoopEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type in [
            'accounts.created',
            'users.created',
            'auth.tokens.created',
            'deposits.created',
            'kyc.created',
            'transactions.created',
        ]

    def handle(self, event_payload: dict):
        pass