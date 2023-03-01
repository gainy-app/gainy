import datetime
import enum

import re
from decimal import Decimal

import dateutil.parser
import pytz

from gainy.data_access.models import classproperty
from gainy.trading.drivewealth.models import BaseDriveWealthModel
from gainy.trading.drivewealth.provider.base import normalize_symbol
from trading.models import ProfileKycStatus, KycStatus, TradingStatementType
from gainy.trading.models import TradingCollectionVersion, TradingOrderStatus

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
    symbol_normalized = None
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
        self.symbol_normalized = normalize_symbol(data["symbol"])
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
