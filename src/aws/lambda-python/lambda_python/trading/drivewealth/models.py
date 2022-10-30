import enum
import re
import requests
from abc import ABC
import dateutil.parser

from gainy.data_access.models import classproperty
from gainy.trading.drivewealth.models import BaseDriveWealthModel
from trading.models import ProfileKycStatus, KycStatus, \
    TradingCollectionVersion, TradingCollectionVersionStatus, TradingMoneyFlowStatus

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


class DriveWealthInstrumentStatus(str, enum.Enum):
    ACTIVE = "ACTIVE"


class DriveWealthInstrument(BaseDriveWealthModel):
    ref_id = None
    symbol = None
    status = None
    data = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    def set_from_response(self, data=None):
        if not data:
            return

        self.ref_id = data.get("id") or data.get("instrumentID")
        self.symbol = data["symbol"]
        self.status = data["status"]
        self.data = data

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_instruments"


class DriveWealthAutopilotRun(BaseDriveWealthModel):
    ref_id = None
    status = None
    account_id = None
    data = None
    orders_outcome = None
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

        if "orders" in self.data:
            # TODO remove verify after DW fixes cert issue
            # TODO move to SQS
            self.orders_outcome = requests.get(self.data["orders"]["outcome"],
                                               verify=False).text

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_autopilot_runs"

    def update_trading_collection_version(
            self, trading_collection_version: TradingCollectionVersion):
        # SUCCESS	Complete Autopilot run successful
        if self.status == "SUCCESS":
            trading_collection_version.set_status(
                TradingCollectionVersionStatus.EXECUTED_FULLY)
            return
        '''
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
        '''
        if re.search(r'(FAILED|TIMEDOUT|ABORTED)$', self.status):
            trading_collection_version.set_status(
                TradingCollectionVersionStatus.FAILED)
            return
        '''
        REBALANCE_NOT_STARTED            rebalance has not yet started
        REBALANCE_REVIEW_COMPLETED       completed rebalance, only rebalancing review
        REBALANCE_NO_ORDERS_GENERATED    completed rebalance, no master orders generated
        REBALANCE_COMPLETED              rebalance is 100% completed
        REBALANCE_10                     percent of rebalance completed, increases in increments of 10 ("REBALANCE_10" ..... "REBALANCE_90")
        AMOUNTCASH_ORDERS_AGGREGATED     aggregate cash value for required orders
        ORDERQTY_ORDERS_AGGREGATED       quantity of orders when funds or instruments re replaced in portfolio
        ORDERQTY_ORDERS_PREPARED         required buy and sell orders for rebalance are prepared to execute
        AMOUNTCASH_ORDERS_PREPARED       cash amount of required buy and sell orders for rebalance
        SELL_ORDERQTY_ORDERS_SUBMITTED   all required sell orders submitted for execution
        SUBACCOUNT_ORDERS_ADJUSTED       adjustment to amount cash orders after quantity orders have executed
        SELL_AMOUNTCASH_ORDERS_SUBMITTED all required sell orders submitted for execution
        SELL_AMOUNTCASH_ORDERS_COMPLETED all required sell orders successfully executed
        SELL_ORDERQTY_ORDERS_COMPLETED   all required sell orders successfully executed
        BUY_ORDERQTY_ORDERS_SUBMITTED    all required buy orders submitted for execution
        BUY_ORDERQTY_ORDERS_COMPLETED    all required buy orders successfully executed
        BUY_AMOUNTCASH_ORDERS_SUBMITTED  all required buy orders submitted for execution
        BUY_AMOUNTCASH_ORDERS_COMPLETED  all required buy orders successfully executed
        ORDERS_CLEANEDUP                 cleaning up failed master orders
        RIA_NEXT_REBALANCE_UPDATED       next scheduled rebalance updated
        ALLOCATION_PREPARED              required allocations are prepared for processing
        ALLOCATION_SUBMITTED             all required allocations submitted for processing
        ALLOCATION_NOT_STARTED           required allocations have not yet been started
        ALLOCATION_10                    percent of allocation completed, increases in increments of 10 ("ALLOCATION_10" ..... "ALLOCATION_90")
        ALLOCATION_COMPLETED             all required allocations have been completed
        SUBACCOUNT_HOLDINGS_UPDATED      all sub-account holdings within required rebalance updated
        '''
        trading_collection_version.set_status(
            TradingCollectionVersionStatus.PENDING)


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
