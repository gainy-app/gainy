import json
import logging
from common.hasura_function import HasuraAction
from services import BillingService, RevenueCatService
from dateutil import parser
from psycopg2.extras import execute_values

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class UpdatePurchases(HasuraAction):

    def __init__(self):
        super().__init__("update_purchases", "profile_id")
        self.revenue_cat_service = RevenueCatService()
        self.billing_service = BillingService()

    def apply(self, db_conn, input_params, headers):
        profile_id = input_params["profile_id"]
        values = []
        existing_revenuecat_ref_ids = []

        subscriber_data = self.revenue_cat_service.get_subscriber(profile_id)
        for entitlement_id, entitlement in subscriber_data[
                'entitlements'].items():
            purchase_date = parser.parse(entitlement['purchase_date'])
            expires_date = parser.parse(entitlement['expires_date'])
            values.append(
                (profile_id, False, expires_date - purchase_date,
                 entitlement_id, json.dumps(entitlement), purchase_date))
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

        self.billing_service.recalculate_subscription_status(
            db_conn, profile_id)

        with db_conn.cursor() as cursor:
            cursor.execute(
                "select subscription_end_date from app.profiles where id = %(profile_id)s",
                {"profile_id": profile_id})
            subscription_end_date = cursor.fetchone()[0]

        return {"subscription_end_date": subscription_end_date.isoformat()}
