import logging
from common.hasura_function import HasuraAction
from services.revenue_cat import RevenueCatService
from gainy.data_access.repository import Repository
from models.billing import Subscription
from service.billing import BillingService
from dateutil import parser

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class UpdatePurchases(HasuraAction):

    def __init__(self):
        super().__init__("update_purchases", "profile_id")
        self.revenue_cat_service = RevenueCatService()
        self.repository = Repository()
        self.billing_service = BillingService()

    def apply(self, db_conn, input_params, headers):
        profile_id = input_params["profile_id"]
        entities = []

        subscriber_data = self.revenue_cat_service.get_subscriber(profile_id)
        for entitlement_id, entitlement in subscriber_data['entitlements'].items():
            purchase_date = parser.parse(entitlement.purchase_date)
            expires_date = parser.parse(entitlement.expires_date)
            entity = Subscription()
            entity.profile_id = profile_id
            entity.is_promotion = False
            entity.period = expires_date - purchase_date
            entity.revenuecat_ref_id = entitlement_id
            entity.revenuecat_entitlement_data = entitlement
            entity.created_at = purchase_date
            entities.append(entity)

        self.repository.persist(db_conn, entities)

        with db_conn.cursor() as cursor:
            cursor.execute(
                """
                delete from app.subscriptions
                where profile_id = %(profile_id)s
                  and revenuecat_ref_id is not null
                  and revenuecat_ref_id not in %(revenuecat_ref_ids)s""",
                {"profile_id": profile_id, revenuecat_ref_ids: tuple([entity.revenuecat_ref_id for entity in entities]})
        self.billing_service.recalculate_subscription_status(db_conn, profile_id)