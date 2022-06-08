import json
import logging
import re
from common.hasura_function import HasuraAction
from services import BillingService, RevenueCatService
import datetime
import dateutil
from psycopg2.extras import execute_values
from gainy.utils import env

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class UpdatePurchases(HasuraAction):

    def __init__(self):
        super().__init__("update_purchases", "profile_id")
        self.revenue_cat_service = RevenueCatService()
        self.billing_service = BillingService()

    def apply(self, db_conn, input_params, headers):
        profile_id = input_params["profile_id"]

        if env() in ['production', 'local']:
            self.sync_revenuecat(db_conn, profile_id)

        self.billing_service.recalculate_subscription_status(
            db_conn, profile_id)

        with db_conn.cursor() as cursor:
            cursor.execute(
                "select subscription_end_date from app.profiles where id = %(profile_id)s",
                {"profile_id": profile_id})
            subscription_end_date = cursor.fetchone()[0]

        return {"subscription_end_date": subscription_end_date.isoformat()}

    def sync_revenuecat(self, db_conn, profile_id):
        values = []
        existing_revenuecat_ref_ids = []

        subscriber_data = self.revenue_cat_service.get_subscriber(profile_id)
        for entitlement_id, entitlement in subscriber_data[
                'entitlements'].items():
            duration = self.get_duration(entitlement['product_identifier'])

            purchase_date = dateutil.parser.parse(entitlement['purchase_date'])
            values.append((profile_id, False, duration, entitlement_id,
                           json.dumps(entitlement), purchase_date))
            existing_revenuecat_ref_ids.append(entitlement_id)

        with db_conn.cursor() as cursor:
            query = """
            INSERT INTO "app"."subscriptions" ("profile_id", "is_promotion", "period", "revenuecat_ref_id", "revenuecat_entitlement_data", "created_at")
            VALUES %s
            ON CONFLICT (profile_id, revenuecat_ref_id)
                DO UPDATE SET "period"                      = excluded."period",
                              "revenuecat_entitlement_data" = excluded."revenuecat_entitlement_data",
                              "created_at"                  = excluded."created_at"
            """
            execute_values(cursor, query, values)

            query = "delete from app.subscriptions where profile_id = %(profile_id)s and revenuecat_ref_id is not null"
            params = {"profile_id": profile_id}
            if existing_revenuecat_ref_ids:
                query += " and revenuecat_ref_id not in %(revenuecat_ref_ids)s"
                params["revenuecat_ref_ids"] = tuple(
                    existing_revenuecat_ref_ids)
            cursor.execute(query, params)

    def get_duration(self, product_identifier):
        product_identifier = product_identifier.lower()
        parts = product_identifier.split('_')
        for part in parts:
            match = re.match(r'^([dwmy])(\d+)$', part)
            if match is None:
                continue

            duration_measure = match[1]
            duration_count = int(match[2])
            if duration_measure == 'd':
                return datetime.timedelta(days=duration_count)
            elif duration_measure == 'w':
                return datetime.timedelta(days=duration_count * 7)
            elif duration_measure == 'm':
                return datetime.timedelta(days=duration_count * 30)
            elif duration_measure == 'y':
                return datetime.timedelta(days=duration_count * 365)

        raise Exception('Product Identifier not recognized')
