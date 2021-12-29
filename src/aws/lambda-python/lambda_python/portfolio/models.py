from abc import ABC, abstractmethod
import datetime


class BaseModel(ABC):

    def to_dict(self):
        return self.__dict__

    def normalize(self):
        d = self.to_dict().copy()
        for i in self.normalization_excluded_fields():
            del d[i]

        for k, i in d.items():
            if isinstance(i, datetime.datetime):
                d[k] = i.isoformat()

        return d

    @property
    @abstractmethod
    def schema_name(self):
        pass

    @property
    @abstractmethod
    def table_name(self):
        pass

    def unique_field_names(self):
        return []

    def unique_id(self):
        values_dict = self.to_dict()
        return '_'.join([values_dict[i] for i in self.unique_field_names()])

    def db_excluded_fields(self):
        return ['created_at', 'updated_at']

    def normalization_excluded_fields(self):
        return []

    def id_field(self):
        return 'id'


class HoldingData(BaseModel):
    id = None
    iso_currency_code = None
    quantity = None
    ref_id = None
    security_ref_id = None
    account_ref_id = None
    security_id = None
    profile_id = None
    account_id = None

    def schema_name(self):
        return 'app'

    def table_name(self):
        return 'profile_holdings'

    def db_excluded_fields(self):
        return ['security_ref_id', 'account_ref_id']

    def normalization_excluded_fields(self):
        return ['security_ref_id', 'account_ref_id']

    def unique_field_names(self):
        return ['ref_id']


class TransactionData(BaseModel):
    id = None

    amount = None
    date = None
    fees = None
    iso_currency_code = None
    name = None
    price = None
    quantity = None
    subtype = None
    type = None

    ref_id = None
    account_ref_id = None
    security_ref_id = None

    security_id = None
    profile_id = None
    account_id = None

    created_at = None
    updated_at = None

    def schema_name(self):
        return 'app'

    def table_name(self):
        return 'profile_portfolio_transactions'

    def db_excluded_fields(self):
        return ['security_ref_id', 'account_ref_id']

    def normalization_excluded_fields(self):
        return ['security_ref_id', 'account_ref_id']

    def unique_field_names(self):
        return ['ref_id']


class Security(BaseModel):
    id = None
    close_price = None
    close_price_as_of = None
    iso_currency_code = None
    name = None
    ref_id = None
    ticker_symbol = None
    type = None
    created_at = None
    updated_at = None

    def schema_name(self):
        return 'app'

    def table_name(self):
        return 'portfolio_securities'

    def unique_field_names(self):
        return ['ref_id']


class Account(BaseModel):
    ref_id = None
    balance_available = None
    balance_current = None
    balance_iso_currency_code = None
    balance_limit = None
    mask = None
    name = None
    official_name = None
    subtype = None
    type = None
    profile_id = None
    created_at = None
    updated_at = None

    def schema_name(self):
        return 'app'

    def table_name(self):
        return 'profile_portfolio_accounts'

    def unique_field_names(self):
        return ['ref_id']
