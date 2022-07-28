import json
import logging
import re
from actions.update_purchases import UpdatePurchases
from common.hasura_function import HasuraAction
from services import BillingService, RevenueCatService
import datetime
import dateutil
from psycopg2.extras import execute_values
from gainy.utils import env

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class UsePromocode(UpdatePurchases):

    def __init__(self):
        super().__init__("use_promocode")


    def apply(self, db_conn, input_params, headers):
        profile_id = input_params["profile_id"]
        promocode = input_params["promocode"]
        tariff = input_params["tariff"]

        self.sync_revenuecat(db_conn, profile_id)

        self.billing_service.recalculate_subscription_status(
            db_conn, profile_id)



        return self.get_response(db_conn)