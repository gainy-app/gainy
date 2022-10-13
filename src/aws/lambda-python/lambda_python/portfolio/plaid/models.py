import json
from gainy.data_access.models import BaseModel, classproperty


class BasePlaidModel(BaseModel):

    @classproperty
    def schema_name(self) -> str:
        return "app"


class PlaidAccount(BasePlaidModel):
    account_id = None
    balance_available = None
    balance_current = None
    iso_currency_code = None
    balance_limit = None
    unofficial_currency_code = None
    mask = None
    name = None
    official_name = None
    subtype = None
    type = None

    def __init__(self, data=None):
        if data:
            self.account_id = data["account_id"]
            self.balance_available = data["balances"]["available"]
            self.balance_current = data["balances"]["current"]
            self.iso_currency_code = data["balances"]["iso_currency_code"]
            self.balance_limit = data["balances"]["limit"]
            self.unofficial_currency_code = data["balances"][
                "unofficial_currency_code"]
            self.mask = data["mask"]
            self.name = data["name"]
            self.official_name = data["official_name"]
            self.subtype = str(data["subtype"])
            self.type = str(data["type"])

    key_fields = ["id"]

    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["id", "created_at"]

    @classproperty
    def table_name(self) -> str:
        return "profile_plaid_access_tokens"


class PlaidAccessToken(BasePlaidModel):
    id = None
    profile_id = None
    access_token = None
    item_id = None
    created_at = None
    institution_id = None
    needs_reauth_since = None
    purpose = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["id", "created_at"]

    @classproperty
    def table_name(self) -> str:
        return "profile_plaid_access_tokens"
