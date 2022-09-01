from gainy.data_access.models import BaseModel, classproperty


# TODO move to compute
class BaseModel2(BaseModel):

    def __init__(self, row: dict = None):
        if row:
            self.set_from_dict(row)

    def set_from_dict(self, row: dict = None):
        if not row:
            return

        for k, v in row.items():
            self.__dict__[k] = v


class DriveWealthUser(BaseModel2):
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
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_users"


class DriveWealthDocument(BaseModel2):
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
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_documents"


class DriveWealthAccount(BaseModel2):
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
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_accounts"
