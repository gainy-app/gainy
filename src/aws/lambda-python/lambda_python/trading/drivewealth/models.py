import datetime
import enum

import re
from abc import ABC
from decimal import Decimal

import dateutil.parser
import pytz

from gainy.data_access.models import classproperty
from gainy.trading.drivewealth.models import BaseDriveWealthModel, DriveWealthRedemptionStatus
from trading.models import ProfileKycStatus, KycStatus, TradingStatementType
from gainy.trading.models import TradingCollectionVersion, TradingOrderStatus, TradingMoneyFlowStatus

PRECISION = 1e-3


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


class DriveWealthBankAccount(BaseDriveWealthModel):
    ref_id = None
    drivewealth_user_id = None
    funding_account_id = None
    plaid_access_token_id = None
    plaid_account_id = None
    status = None
    bank_account_nickname = None
    bank_account_number = None
    bank_routing_number = None
    holder_name = None
    bank_account_type = None
    data = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    def set_from_response(self, data=None):
        if not data:
            return
        self.ref_id = data['id']
        self.status = data["status"]

        details = data["bankAccountDetails"]
        self.bank_account_nickname = details['bankAccountNickname']
        self.bank_account_number = details['bankAccountNumber']
        self.bank_routing_number = details['bankRoutingNumber']
        self.bank_account_type = details.get('bankAccountType')
        self.data = data

        if "userDetails" in data:
            self.drivewealth_user_id = data["userDetails"]['userID']
            self.holder_name = " ".join([
                data["userDetails"]['firstName'],
                data["userDetails"]['lastName']
            ])

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_bank_accounts"


class DriveWealthAccountStatus(str, enum.Enum):
    OPEN = 'OPEN'


class BaseDriveWealthMoneyFlowModel(BaseDriveWealthModel, ABC):
    ref_id = None
    trading_account_ref_id = None
    bank_account_ref_id = None
    money_flow_id = None
    data = None
    status = None
    fees_total_amount: Decimal = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    def set_from_response(self, data=None):
        self.ref_id = data.get("id") or data.get("paymentID")
        if "accountDetails" in data:
            self.trading_account_ref_id = data["accountDetails"]["accountID"]
        elif "accountID" in data:
            self.trading_account_ref_id = data["accountID"]

        if "statusMessage" in data:
            self.status = data["statusMessage"]
        else:
            self.status = data["status"]["message"]

        self.data = data

    def is_pending(self) -> bool:
        return self.status in [
            'Started', DriveWealthRedemptionStatus.RIA_Pending.name, 'Pending',
            'Other', 'On Hold'
        ]

    def is_approved(self) -> bool:
        return self.status in [
            'Approved', DriveWealthRedemptionStatus.RIA_Approved.name
        ]

    def get_money_flow_status(self) -> TradingMoneyFlowStatus:
        """
        Started	0	"STARTED"
        Pending	1	"PENDING"	Every new deposit for a self-directed account is set to "Pending". From here, the deposit can be marked as "Rejected", "On Hold" or "Approved".
        Successful	2	"SUCCESSFUL"	After a deposit is marked "Approved", the next step is "Successful".
        Failed	3	"FAILED"	If a deposit is marked as "Rejected", the deposit will immediately be set to "Failed".
        Other	4	"OTHER"
        RIA Pending	11	"RIA_Pending"
        RIA Approved	12	"RIA_Approved"
        RIA Rejected	13	"RIA_Rejected"
        Approved	14	"APPROVED"	Once marked as "Approved", the deposit will be processed.
        Rejected	15	"REJECTED"	Updating a deposit to "Rejected" will immediately set it 's status to "Failed"
        On Hold	16	"ON_HOLD"	The "On Hold" status is reserved for deposits that aren't ready to be processed.
        Returned	5	"RETURNED"	A deposit is marked as returned if DW receives notification from our bank that the deposit had failed.
        Unknown	-1	–	Reserved for errors.
        """

        if self.is_pending():
            return TradingMoneyFlowStatus.PENDING
        if self.is_approved():
            return TradingMoneyFlowStatus.APPROVED
        if self.status == DriveWealthRedemptionStatus.Successful.name:
            return TradingMoneyFlowStatus.SUCCESS
        return TradingMoneyFlowStatus.FAILED


class DriveWealthDeposit(BaseDriveWealthMoneyFlowModel):

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_deposits"


class DriveWealthRedemption(BaseDriveWealthMoneyFlowModel):

    def set_from_response(self, data=None):
        if not data:
            return

        if "fees" in data:
            fees_total_amount = Decimal(0)
            for fee in data["fees"]:
                fees_total_amount += Decimal(fee["amount"])
            self.fees_total_amount = fees_total_amount

        super().set_from_response(data)

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_redemptions"


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

    def update_trading_collection_version(
            self, trading_collection_version: TradingCollectionVersion):

        if self.is_successful():
            trading_collection_version.set_status(
                TradingOrderStatus.EXECUTED_FULLY)
        elif self.is_failed():
            trading_collection_version.set_status(TradingOrderStatus.FAILED)


class DriveWealthOrder(BaseDriveWealthModel):
    ref_id = None
    status = None  # NEW, PARTIAL_FILL, CANCELLED, REJECTED, FILLED
    account_id = None
    symbol = None
    data = None
    last_executed_at = None
    total_order_amount_normalized: Decimal = None
    date: datetime.date = None
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
        self.account_id = data["accountID"]
        self.symbol = data["symbol"]
        if "lastExecuted" in data:
            self.last_executed_at = dateutil.parser.parse(data["lastExecuted"])
            self.date = self.last_executed_at.astimezone(
                pytz.timezone('America/New_York')).date()
        self.total_order_amount_normalized = abs(
            Decimal(data['totalOrderAmount'])) * (-1 if data['side'] == 'SELL'
                                                  else 1)
        self.data = data

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_orders"

    def is_filled(self) -> bool:
        return self.status == "FILLED"

    def is_rejected(self):
        return self.status == "REJECTED"


class DriveWealthKycStatus:
    data = None

    def __init__(self, data: dict):
        self.data = data

    def get_profile_kyc_status(self) -> ProfileKycStatus:
        kyc = self.data["kyc"]
        kyc_status = self.map_dw_kyc_status(kyc["status"]["name"])
        message = kyc["status"].get("name") or kyc.get("statusComment")

        errors = kyc.get("errors", [])
        error_messages = list(map(lambda e: e['description'], errors))

        entity = ProfileKycStatus()
        entity.status = kyc_status
        entity.message = message
        entity.error_messages = error_messages
        entity.created_at = dateutil.parser.parse(kyc["updated"])
        return entity

    @staticmethod
    def map_dw_kyc_status(kyc_status):
        if kyc_status == "KYC_NOT_READY":
            return KycStatus.NOT_READY
        if kyc_status == "KYC_READY":
            return KycStatus.READY
        if kyc_status == "KYC_PROCESSING":
            return KycStatus.PROCESSING
        if kyc_status == "KYC_APPROVED":
            return KycStatus.APPROVED
        if kyc_status == "KYC_INFO_REQUIRED":
            return KycStatus.INFO_REQUIRED
        if kyc_status == "KYC_DOC_REQUIRED":
            return KycStatus.DOC_REQUIRED
        if kyc_status == "KYC_MANUAL_REVIEW":
            return KycStatus.MANUAL_REVIEW
        if kyc_status == "KYC_DENIED":
            return KycStatus.DENIED
        raise Exception('Unknown kyc status %s' % kyc_status)


class DriveWealthStatement(BaseDriveWealthModel):
    file_key: str = None
    trading_statement_id: int = None
    type: TradingStatementType = None
    display_name: str = None
    account_id: str = None
    user_id: str = None
    created_at: datetime.datetime = None

    key_fields = ["account_id", "type", "file_key"]

    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["created_at"]

    def set_from_dict(self, row: dict = None):
        super().set_from_dict(row)

        if row and row["type"]:
            self.type = TradingStatementType(row["type"])
        return self

    def set_from_response(self, data: dict = None):
        if not data:
            return
        self.display_name = data["displayName"]
        self.file_key = data["fileKey"]
        self.data = data

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_statements"
