from typing import Dict, Optional
from decimal import Decimal
import enum
from gainy.data_access.models import BaseModel, classproperty


class KycStatus(str, enum.Enum):
    NOT_READY = "NOT_READY"
    READY = "READY"
    PROCESSING = "PROCESSING"
    APPROVED = "APPROVED"
    INFO_REQUIRED = "INFO_REQUIRED"
    DOC_REQUIRED = "DOC_REQUIRED"
    MANUAL_REVIEW = "MANUAL_REVIEW"
    DENIED = "DENIED"


class ProfileKycStatus:
    status = None
    message = None
    updated_at = None

    def __init__(self, status: str, message: Optional[str]):
        self.status = KycStatus(status)
        self.message = message


class KycDocument:
    uploaded_file_id = None
    type = None
    side = None
    content_type = None

    def __init__(self, uploaded_file_id, type, side):
        self.uploaded_file_id = uploaded_file_id
        self.type = type
        self.side = side


class TradingAccount(BaseModel):
    id = None
    profile_id = None
    name = None
    cash_available_for_trade = None
    cash_available_for_withdrawal = None
    cash_balance = None
    created_at = None
    updated_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "trading_accounts"


class FundingAccount(BaseModel):
    id = None
    profile_id = None
    plaid_access_token_id = None
    plaid_account_id = None
    name = None
    balance = None
    created_at = None
    updated_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "trading_funding_accounts"


class TradingMoneyFlow(BaseModel):
    id = None
    profile_id = None
    status = None
    amount = None
    trading_account_id = None
    funding_account_id = None
    created_at = None
    updated_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    def __init__(self, profile_id: int, amount: Decimal,
                 trading_account_id: int, funding_account_id: int):
        self.profile_id = profile_id
        self.amount = amount
        self.trading_account_id = trading_account_id
        self.funding_account_id = funding_account_id

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "trading_money_flow"


class TradingCollectionVersion(BaseModel):
    id = None
    profile_id = None
    collection_id = None
    target_amount_delta = None
    weights = None
    created_at = None
    updated_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    def __init__(self, profile_id: int, collection_id: int,
                 weights: Dict[str, Decimal], target_amount_delta: Decimal):
        self.profile_id = profile_id
        self.collection_id = collection_id
        self.target_amount_delta = target_amount_delta
        self.weights = weights

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "trading_collection_versions"


class CollectionHoldingStatus:
    symbol = None
    target_weight = None
    actual_weight = None
    value = None

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "target_weight": self.target_weight,
            "actual_weight": self.actual_weight,
            "value": self.value,
        }
