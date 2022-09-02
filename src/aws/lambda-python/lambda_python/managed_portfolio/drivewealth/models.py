import json
from gainy.data_access.models import BaseModel, classproperty


class BaseDriveWealthModel(BaseModel):

    @classproperty
    def schema_name(self) -> str:
        return "app"

    def to_dict(self) -> str:
        return {
            **super().to_dict(),
            "data": json.dumps(self.data),
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
    managed_portfolio_funding_account_id = None
    plaid_access_token_id = None
    bank_account_nickname = None
    bank_account_number = None
    bank_routing_number = None
    data = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_bank_accounts"
