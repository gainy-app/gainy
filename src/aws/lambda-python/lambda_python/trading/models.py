import json
from typing import Dict, Optional
from decimal import Decimal
import enum

from common import DecimalEncoder
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


class KycDocument(BaseModel):
    id = None
    profile_id = None
    uploaded_file_id = None
    type = None
    side = None
    content_type = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "kyc_documents"


class TradingAccount(BaseModel):
    id = None
    profile_id = None
    name = None
    cash_available_for_trade = None
    cash_available_for_withdrawal = None
    cash_balance = None
    equity_value = None
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

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "trading_money_flow"


class TradingCollectionVersionStatus(enum.Enum):
    STATUS_PENDING = "PENDING"
    STATUS_SUCCESS = "SUCCESS"
    STATUS_FAILED = "FAILED"


class TradingCollectionVersion(BaseModel):
    id = None
    profile_id = None
    collection_id = None
    status = None
    target_amount_delta = None
    weights: Dict[str, Decimal] = None
    created_at = None
    updated_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    def __init__(self, row=None):
        super().__init__(row)

        if not row:
            return

        self.status = TradingCollectionVersionStatus[
            row["status"]] if row["status"] else None

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "trading_collection_versions"

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "status": self.status.name if self.status else None,
            "weights": json.dumps(self.weights, cls=DecimalEncoder),
        }


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
