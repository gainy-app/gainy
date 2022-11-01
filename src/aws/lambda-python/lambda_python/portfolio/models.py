from gainy.data_access.models import BaseModel

from abc import ABC
import datetime


class PortfolioBaseModel(BaseModel, ABC):

    def normalize(self):
        d = self.to_dict().copy()
        for i in self.normalization_excluded_fields:
            del d[i]

        for k, i in d.items():
            if isinstance(i, datetime.datetime):
                d[k] = i.isoformat()

        return d

    def unique_id(self):
        values_dict = self.to_dict()
        return '_'.join([values_dict[i] for i in self.key_fields])

    @property
    def db_excluded_fields(self):
        return ['id', 'created_at', 'updated_at']

    @property
    def non_persistent_fields(self):
        return ['id', 'created_at', 'updated_at']

    @property
    def normalization_excluded_fields(self):
        return []


class HoldingData(PortfolioBaseModel):
    id = None
    iso_currency_code = None
    quantity = None
    ref_id = None
    security_ref_id = None
    account_ref_id = None
    security_id = None
    profile_id = None
    account_id = None
    plaid_access_token_id = None

    @property
    def schema_name(self):
        return 'app'

    @property
    def table_name(self):
        return 'profile_holdings'

    @property
    def db_excluded_fields(self):
        return super().db_excluded_fields + [
            'security_ref_id', 'account_ref_id'
        ]

    @property
    def normalization_excluded_fields(self):
        return ['security_ref_id', 'account_ref_id']

    @property
    def key_fields(self):
        return ['ref_id']


class TransactionData(PortfolioBaseModel):
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
    plaid_access_token_id = None

    created_at = None
    updated_at = None

    @property
    def schema_name(self):
        return 'app'

    @property
    def table_name(self):
        return 'profile_portfolio_transactions'

    @property
    def db_excluded_fields(self):
        return super().db_excluded_fields + [
            'security_ref_id', 'account_ref_id'
        ]

    @property
    def normalization_excluded_fields(self):
        return ['security_ref_id', 'account_ref_id']

    @property
    def key_fields(self):
        return ['ref_id']


class Security(PortfolioBaseModel):
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

    @property
    def schema_name(self):
        return 'app'

    @property
    def table_name(self):
        return 'portfolio_securities'

    @property
    def key_fields(self):
        return ['ref_id']


class Account(PortfolioBaseModel):
    id = None
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
    plaid_access_token_id = None

    @property
    def schema_name(self):
        return 'app'

    @property
    def table_name(self):
        return 'profile_portfolio_accounts'

    @property
    def key_fields(self):
        return ['ref_id']


class Institution(PortfolioBaseModel):
    id = None
    ref_id = None
    name = None
    primary_color = None
    url = None
    logo = None
    created_at = None
    updated_at = None

    @property
    def schema_name(self):
        return 'app'

    @property
    def table_name(self):
        return 'plaid_institutions'

    @property
    def key_fields(self):
        return ['ref_id']


class PortfolioChartFilter:
    periods = None
    access_token_ids = None
    broker_ids = None
    institution_ids = None
    interest_ids = None
    category_ids = None
    security_types = None
    ltt_only = None
