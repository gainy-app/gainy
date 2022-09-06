from decimal import Decimal
import json
from gainy.data_access.models import BaseModel, classproperty

PRECISION = 1e-3


class DecimalEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


class BaseDriveWealthModel(BaseModel):

    @classproperty
    def schema_name(self) -> str:
        return "app"

    def to_dict(self) -> str:
        return {
            **super().to_dict(),
            "data": json.dumps(self.data, cls=DecimalEncoder),
        }


class DriveWealthUser(BaseDriveWealthModel):
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
        return "drivewealth_users"


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


class DriveWealthAccount(BaseDriveWealthModel):
    ref_id = None
    drivewealth_user_id = None
    trading_account_id = None
    portfolio_id = None
    status = None
    ref_no = None
    nickname = None
    cash_available_for_trade = None
    cash_available_for_withdrawal = None
    cash_balance = None
    data = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_accounts"


class DriveWealthBankAccount(BaseDriveWealthModel):
    ref_id = None
    drivewealth_user_id = None
    funding_account_id = None
    plaid_access_token_id = None
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

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_bank_accounts"


class BaseDriveWealthMoneyFlowModel(BaseDriveWealthModel):
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

    def __init__(self, data=None):
        super().__init__()

        if not data:
            return

        self.ref_id = data["id"]
        self.status = data["status"]["message"]
        self.data = data


class DriveWealthDeposit(BaseDriveWealthMoneyFlowModel):

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_deposits"


class DriveWealthRedemption(BaseDriveWealthMoneyFlowModel):

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_redemptions"


class DriveWealthFund(BaseDriveWealthModel):
    ref_id = None
    drivewealth_user_id = None
    collection_id = None
    trading_collection_version_id = None
    weights = None
    holdings = []
    data = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    def __init__(self, data=None):
        super().__init__()

        if not data:
            return

        self.ref_id = data["id"]
        self.drivewealth_user_id = data["userID"]
        self.data = data
        self.holdings = self.data["holdings"]

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_funds"


class DriveWealthPortfolioStatus(BaseDriveWealthModel):
    id = None
    drivewealth_portfolio_id = None
    cash_value: Decimal = None
    cash_actual_weight: Decimal = None
    holdings = None
    data = None
    created_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["id", "created_at"]

    def __init__(self, data: dict):
        super().__init__()
        self.drivewealth_portfolio_id = data["id"]
        self.data = data

        self.holdings = {}
        for i in data["holdings"]:
            if i["type"] == "CASH_RESERVE":
                self.cash_value = Decimal(i["value"])
                self.cash_actual_weight = Decimal(i["actual"])
            else:
                self.holdings[i["id"]] = i

    def get_fund_value(self, fund_ref_id) -> Decimal:
        if not self.holdings or fund_ref_id not in self.holdings:
            return Decimal(0)

        return Decimal(self.holdings[fund_ref_id]["value"])

    def get_fund_actual_weight(self, fund_ref_id) -> Decimal:
        if not self.holdings or fund_ref_id not in self.holdings:
            return Decimal(0)

        return Decimal(self.holdings[fund_ref_id]["actual"])

    def get_fund_ref_ids(self) -> list:
        if not self.holdings:
            return []

        return list(self.holdings.keys())

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_portfolio_statuses"


class DriveWealthPortfolio(BaseDriveWealthModel):
    ref_id = None
    drivewealth_user_id = None
    drivewealth_account_id = None
    cash_target_weight = None
    holdings = None
    data = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    def __init__(self, data=None):
        super().__init__()

        if not data:
            return

        self.ref_id = data["id"]
        self.drivewealth_user_id = data["userID"]
        self.data = data

        self.cash_target_weight = 1
        self.holdings = {}
        for i in data["holdings"]:
            if i["type"] == "CASH_RESERVE":
                self.cash_target_weight = i["target"]
            else:
                self.holdings[i["instrumentID"]] = i["target"]

    def set_target_weights_from_status_actual_weights(
            self, portfolio_status: DriveWealthPortfolioStatus):
        self.cash_target_weight = portfolio_status.cash_actual_weight

        for fund_ref_id, i in self.holdings.items():
            self.holdings[fund_ref_id] = 0

        for fund_ref_id in portfolio_status.get_fund_ref_ids():
            self.holdings[
                fund_ref_id] = portfolio_status.get_fund_actual_weight(
                    fund_ref_id)

    def move_cash_to_fund(self, fund: DriveWealthFund, weight_delta: Decimal):
        cash_weight = self.cash_target_weight
        if cash_weight - weight_delta < -PRECISION:
            raise Exception('cash weight can not be negative')
        if cash_weight - weight_delta > 1 + PRECISION:
            raise Exception('cash weight can not be greater than 1')

        fund_weight = self.get_fund_weight(fund.ref_id)
        if fund_weight + weight_delta < -PRECISION:
            raise Exception('fund weight can not be negative')
        if fund_weight + weight_delta > 1 + PRECISION:
            raise Exception('fund weight can not be greater than 1')

        cash_weight -= weight_delta
        self.cash_target_weight = min(1, max(0, cash_weight))

        fund_weight += weight_delta
        self.set_fund_weight(fund, min(1, max(0, fund_weight)))

    def get_fund_weight(self, fund_ref_id: str) -> Decimal:
        if not self.holdings or fund_ref_id not in self.holdings:
            return 0

        return self.holdings[fund_ref_id]

    def set_fund_weight(self, fund: DriveWealthFund, weight: Decimal):
        if not self.holdings:
            self.holdings = {}

        self.holdings[fund.ref_id] = weight

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_portfolios"


class DriveWealthAutopilotRun(BaseDriveWealthModel):
    ref_id = None
    status = None
    accounts = None
    data = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    def __init__(self, data: dict):
        super().__init__()
        self.ref_id = data["id"]
        self.status = data["status"]
        self.data = data

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_autopilot_runs"

    def to_dict(self) -> str:
        return {
            **super().to_dict(),
            "accounts": json.dumps(self.accounts),
        }
