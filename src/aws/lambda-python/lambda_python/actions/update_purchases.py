import json
import re
from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from services import BillingService, RevenueCatService
import datetime
import dateutil.parser
from psycopg2.extras import execute_values
from gainy.utils import env, get_logger

logger = get_logger(__name__)


class UpdatePurchases(HasuraAction):

    def __init__(self, action_name="update_purchases"):
        super().__init__(action_name, "profile_id")
        self.revenue_cat_service = RevenueCatService()
        self.billing_service = BillingService()

    def apply(self, input_params, context_container: ContextContainer):
        db_conn = context_container.db_conn
        profile_id = input_params["profile_id"]

        self.sync_revenuecat(db_conn, profile_id)

        self.billing_service.recalculate_subscription_status(
            db_conn, profile_id)

        return self.get_response(db_conn, profile_id)

    def get_response(self, db_conn, profile_id):
        with db_conn.cursor() as cursor:
            cursor.execute(
                "select subscription_end_date from app.profiles where id = %(profile_id)s",
                {"profile_id": profile_id})
            subscription_end_date = cursor.fetchone()[0]

        return {
            "subscription_end_date":
            subscription_end_date.isoformat()
            if subscription_end_date else None
        }

    def sync_revenuecat(self, db_conn, profile_id):
        if env() not in ['production', 'local']:
            return

        values = []
        existing_ref_ids = []

        subscriber_data = self.revenue_cat_service.get_subscriber(profile_id)
        for product_id, entitlement in subscriber_data['entitlements'].items():
            tariff = entitlement['product_identifier']
            ref_id = tariff + "_" + entitlement["purchase_date"]
            duration = self.get_duration(tariff)

            purchase_date = dateutil.parser.parse(entitlement['purchase_date'])
            values.append(
                (profile_id, False, duration, ref_id, product_id, tariff,
                 json.dumps(entitlement), purchase_date))
            existing_ref_ids.append(ref_id)

        with db_conn.cursor() as cursor:
            query = """
            INSERT INTO "app"."subscriptions" ("profile_id", "is_promotion", "period", "ref_id", "product_id", "tariff", "revenuecat_entitlement_data", "created_at")
            VALUES %s
            ON CONFLICT (profile_id, ref_id)
                DO UPDATE SET "period"                      = excluded."period",
                              "product_id"                  = excluded."product_id",
                              "tariff"                      = excluded."tariff",
                              "revenuecat_entitlement_data" = excluded."revenuecat_entitlement_data",
                              "created_at"                  = excluded."created_at"
            """
            execute_values(cursor, query, values)

            query = "update app.subscriptions set expired_at = now() where profile_id = %(profile_id)s and ref_id is not null"
            params = {"profile_id": profile_id}
            if existing_ref_ids:
                query += " and ref_id not in %(ref_ids)s"
                params["ref_ids"] = tuple(existing_ref_ids)
            cursor.execute(query, params)

    def get_duration(self, product_identifier):
        product_identifier = product_identifier.lower()

        if product_identifier.find("lifetime") > -1:
            return datetime.timedelta(days=100 * 365)

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
