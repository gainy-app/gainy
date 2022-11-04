import enum
import re
from abc import ABC
import dateutil.parser

from gainy.data_access.models import classproperty
from gainy.trading.drivewealth.models import BaseDriveWealthModel
from trading.models import ProfileKycStatus, KycStatus, TradingMoneyFlowStatus
from gainy.trading.models import TradingCollectionVersion, TradingCollectionVersionStatus

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


class BaseDriveWealthMoneyFlowModel(BaseDriveWealthModel, ABC):
    ref_id = None
    trading_account_ref_id = None
    bank_account_ref_id = None
    money_flow_id = None
    data = None
    status = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    def set_from_response(self, data=None):
        self.ref_id = data["id"]
        if "accountDetails" in data:
            self.trading_account_ref_id = data["accountDetails"]["accountID"]
        elif "accountID" in data:
            self.trading_account_ref_id = data["accountID"]
        self.status = data["status"]["message"]
        self.data = data

    def get_money_flow_status(self) -> TradingMoneyFlowStatus:
        """
        Started	0	"STARTED"
        Pending	1	"PENDING"	Every new deposit for a self-directed account is set to "Pending". From here, the deposit can be marked as "Rejected", "On Hold" or "Approved".
        Successful	2	"SUCCESSFUL"	After a deposit is marked "Approved", the next step is "Successful".
        Failed	3	"FAILED"	If a deposit is marked as "Rejected", the deposit will immediately be set to "Failed".
        Other	4	"OTHER"
        Approved	14	"APPROVED"	Once marked as "Approved", the deposit will be processed.
        Rejected	15	"REJECTED"	Updating a deposit to "Rejected" will immediately set it 's status to "Failed"
        On Hold	16	"ON_HOLD"	The "On Hold" status is reserved for deposits that aren't ready to be processed.
        Returned	5	"RETURNED"	A deposit is marked as returned if DW receives notification from our bank that the deposit had failed.
        Unknown	-1	–	Reserved for errors.
        """

        if self.status in [
                'Started', 'Pending', 'Other', 'Approved', 'On Hold'
        ]:
            return TradingMoneyFlowStatus.PENDING
        if self.status in ['Successful']:
            return TradingMoneyFlowStatus.SUCCESS
        return TradingMoneyFlowStatus.FAILED


class DriveWealthDeposit(BaseDriveWealthMoneyFlowModel):

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_deposits"


class DriveWealthRedemption(BaseDriveWealthMoneyFlowModel):

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
                TradingCollectionVersionStatus.EXECUTED_FULLY)
        elif self.is_failed():
            trading_collection_version.set_status(
                TradingCollectionVersionStatus.FAILED)


class DriveWealthKycStatus:
    data = None

    def __init__(self, data: dict):
        self.data = data

    def get_profile_kyc_status(self) -> ProfileKycStatus:
        kyc = self.data["kyc"]
        kyc_status = kyc["status"]["name"]
        if kyc_status == "KYC_NOT_READY":
            status = KycStatus.NOT_READY
        elif kyc_status == "KYC_READY":
            status = KycStatus.READY
        elif kyc_status == "KYC_PROCESSING":
            status = KycStatus.PROCESSING
        elif kyc_status == "KYC_APPROVED":
            status = KycStatus.APPROVED
        elif kyc_status == "KYC_INFO_REQUIRED":
            status = KycStatus.INFO_REQUIRED
        elif kyc_status == "KYC_DOC_REQUIRED":
            status = KycStatus.DOC_REQUIRED
        elif kyc_status == "KYC_MANUAL_REVIEW":
            status = KycStatus.MANUAL_REVIEW
        elif kyc_status == "KYC_DENIED":
            status = KycStatus.DENIED
        else:
            raise Exception('Unknown kyc status %s' % kyc_status)

        errors = kyc.get("errors")
        if errors:
            message = ". ".join(
                map(lambda e: f"{e['description']} ({e['code']})", errors))
        else:
            message = kyc.get("statusComment")

        status = ProfileKycStatus(status, message)
        status.updated_at = dateutil.parser.parse(kyc["updated"])
        return status
