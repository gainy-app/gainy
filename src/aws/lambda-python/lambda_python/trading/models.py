import json
from typing import Dict, Optional
from decimal import Decimal
import enum

from gainy.data_access.models import BaseModel, classproperty, DecimalEncoder
from gainy.exceptions import BadRequestException


class KycStatus(str, enum.Enum):
    NOT_READY = "NOT_READY"
    READY = "READY"
    PROCESSING = "PROCESSING"
    APPROVED = "APPROVED"
    INFO_REQUIRED = "INFO_REQUIRED"
    DOC_REQUIRED = "DOC_REQUIRED"
    MANUAL_REVIEW = "MANUAL_REVIEW"
    DENIED = "DENIED"


class KycDocumentType(str, enum.Enum):
    DRIVER_LICENSE = "DRIVER_LICENSE"
    PASSPORT = "PASSPORT"
    NATIONAL_ID_CARD = "NATIONAL_ID_CARD"
    VOTER_ID = "VOTER_ID"
    WORK_PERMIT = "WORK_PERMIT"
    VISA = "VISA"
    RESIDENCE_PERMIT = "RESIDENCE_PERMIT"


class KycDocumentSide(str, enum.Enum):
    FRONT = "FRONT"
    BACK = "BACK"


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
    type: KycDocumentType = None
    side: KycDocumentSide = None
    content_type = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    def __init__(self, row: dict = None):
        super().__init__(row)

        if row and row["type"]:
            self.type = KycDocumentType(row["type"])

        if row and row["side"]:
            self.side = KycDocumentSide(row["side"])

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "kyc_documents"

    def validate(self):
        if self.type in [KycDocumentType.PASSPORT, KycDocumentType.VISA
                         ] and self.side == KycDocumentSide.BACK:
            raise BadRequestException(
                f"Document of type {self.type} can't have BACK side")


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


class TradingMoneyFlowStatus(enum.Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"

class TradingMoneyFlow(BaseModel):
    id = None
    profile_id = None
    status: TradingMoneyFlowStatus = None
    amount = None
    trading_account_id = None
    funding_account_id = None
    created_at = None
    updated_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    def __init__(self, row=None):
        super().__init__(row)

        if not row:
            return

        self.status = TradingMoneyFlowStatus[
            row["status"]] if row["status"] else None

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "trading_money_flow"

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "status": self.status.name if self.status else None,
        }


class TradingCollectionVersionStatus(enum.Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


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
