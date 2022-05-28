from gainy.data_access.models import BaseModel

from abc import ABC
import datetime


class Subscription(BaseModel):
    id = None
    profile_id = None
    invitation_id = None
    is_promotion = None
    period = None
    revenuecat_ref_id = None
    revenuecat_entitlement_data = None
    created_at = None

    @property
    def schema_name(self):
        return 'app'

    @property
    def table_name(self):
        return 'subscriptions'

    @property
    def key_fields(self):
        return ['invitation_id', 'revenuecat_ref_id']

    @property
    def db_excluded_fields(self):
        return ['id', 'created_at']

    @property
    def non_persistent_fields(self):
        return ['id', 'created_at']

    def unique_id(self):
        values_dict = self.to_dict()
        return '_'.join([values_dict[i] for i in self.key_fields])
