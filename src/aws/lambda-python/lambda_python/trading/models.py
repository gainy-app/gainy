import datetime
import json

from typing import Optional, List
import enum

from gainy.data_access.models import BaseModel, classproperty
from gainy.exceptions import BadRequestException
from gainy.trading.models import TradingMoneyFlowStatus


class TradingStatementType(str, enum.Enum):
    MONTHLY_STATEMENT = "MONTHLY_STATEMENT"
    TAX = "TAX"
    TRADE_CONFIRMATION = "TRADE_CONFIRMATION"


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


class ProfileKycStatus(BaseModel):
    id: int = None
    profile_id: int = None
    status: KycStatus = None
    message: str = None
    error_messages: list[str] = None
    created_at: datetime.datetime = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["id", "created_at"]

    def __init__(self, row: dict = None):
        super().__init__(row)

        if row and row["status"]:
            self.status = KycStatus(row["status"])

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "kyc_statuses"

    def to_dict(self) -> dict:
        return {
            **super().to_dict(), "error_messages":
            json.dumps(self.error_messages)
        }


class TradingStatement(BaseModel):
    id: int = None
    profile_id: int = None
    type: TradingStatementType = None
    display_name: str = None
    created_at: datetime.datetime = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["id", "created_at"]

    def __init__(self, row: dict = None):
        super().__init__(row)

        if row and row["type"]:
            self.type = TradingStatementType(row["type"])

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "trading_statements"


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
            "status":
            self.status.name if self.status else None,
        }
