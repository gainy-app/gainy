from gainy.trading.drivewealth.exceptions import InstrumentNotFoundException
from gainy.utils import get_logger
from trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler

logger = get_logger(__name__)


class InstrumentUpdatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type == 'instruments.updated'

    def handle(self, event_payload: dict):
        try:
            ref_id = event_payload["instrumentID"]
            self.provider.sync_instrument(ref_id=ref_id)
        except InstrumentNotFoundException:
            pass
