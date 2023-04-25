import re

from gainy.data_access.models import classproperty
from gainy.trading.drivewealth.models import BaseDriveWealthModel
from gainy.utils import get_logger

PRECISION = 1e-3
logger = get_logger(__name__)


class DriveWealthDocument(BaseDriveWealthModel):
    ref_id = None
    kyc_document_id = None
    status = None
    data = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_documents"


class DriveWealthAutopilotRun(BaseDriveWealthModel):
    ref_id = None
    status = None
    account_id = None
    data = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    def set_from_response(self, data: dict = None):
        if not data:
            return
        self.ref_id = data["id"]
        self.status = data["status"]
        self.data = data

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_autopilot_runs"

    def is_successful(self):
        # SUCCESS	Complete Autopilot run successful
        return self.status == "SUCCESS"

    def is_failed(self):
        """
        REBALANCE_FAILED                  rebalance has failed
        SELL_AMOUNTCASH_ORDERS_FAILED     one or more sell orders have failed
        SELL_ORDERQTY_ORDERS_FAILED       one or more sell orders have failed
        BUY_ORDERQTY_ORDERS_FAILED        one or more buy orders have failed
        BUY_AMOUNTCASH_ORDERS_FAILED      one or more buy orders have failed
        ALLOCATION_FAILED                 fatal allocation failure
        SUBACCOUNT_HOLDINGS_UPDATE_FAILED sub-account holdings within require rebalance update failed
        SELL_AMOUNTCASH_ORDERS_TIMEDOUT   required sell orders have timed out
        SELL_ORDERQTY_ORDERS_TIMEDOUT     required sell orders have timed out
        BUY_ORDERQTY_ORDERS_TIMEDOUT      required buy orders have timedout
        BUY_AMOUNTCASH_ORDERS_TIMEDOUT    required buy orders have timedout
        ALLOCATION_TIMEDOUT               allocation processing timeout
        REBALANCE_ABORTED                 rebalance has been cancelled
        """
        return re.search(r'(FAILED|TIMEDOUT|ABORTED)$', self.status)
