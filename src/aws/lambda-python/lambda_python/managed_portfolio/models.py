import enum
from gainy.data_access.models import BaseModel, classproperty


class KycStatus(str, enum.Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REVOKED = "REVOKED"
    CLOSED = "CLOSED"


class ProfileKycStatus:
    status = None

    def __init__(self, status: str):
        self.status = KycStatus(status)


class KycDocument:
    uploaded_file_id = None
    uploaded_file_id = None
    type = None
    side = None
    content_type = None

    def __init__(self, uploaded_file_id, type, side):
        self.uploaded_file_id = uploaded_file_id
        self.type = type
        self.side = side


class ManagedPortfolioTradingAccount(BaseModel):
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
        return "managed_portfolio_trading_accounts"


class ManagedPortfolioFundingAccount(BaseModel):
    id = None
    profile_id = None
    plaid_access_token_id = None
    name = None
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
        return "managed_portfolio_funding_accounts"
