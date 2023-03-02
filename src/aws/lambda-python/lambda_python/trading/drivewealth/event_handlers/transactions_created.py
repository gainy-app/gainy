from gainy.trading.drivewealth.models import DriveWealthTransaction
from gainy.utils import get_logger
from trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler

logger = get_logger(__name__)


class TransactionsCreatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type == 'transactions.created'

    def handle(self, event_payload: dict):
        transaction = DriveWealthTransaction()
        transaction.account_id = event_payload["accountID"]
        transaction.set_from_response(event_payload["transaction"])
        self.repo.persist(transaction)

        self.provider.on_new_transaction(transaction.account_id)

        self.sync_trading_account_balances(transaction.account_id, force=True)
