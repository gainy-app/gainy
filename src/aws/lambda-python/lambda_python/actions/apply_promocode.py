from common.context_container import ContextContainer
import logging

from actions.update_purchases import UpdatePurchases

logger = logging.getLogger()


class ApplyPromocode(UpdatePurchases):

    def __init__(self):
        super().__init__("apply_promocode")

    def apply(self, input_params, context_container: ContextContainer):
        db_conn = context_container.db_conn

        response = super().apply(input_params, context_container)

        profile_id = input_params["profile_id"]
        promocode = input_params["promocode"]
        tariff = input_params["tariff"]
        self.apply_promocode(db_conn, profile_id, promocode, tariff)

        return response

    def apply_promocode(self, db_conn, profile_id, promocode, tariff):
        with db_conn.cursor() as cursor:
            cursor.execute(
                "select id from app.promocodes where code = %(code)s",
                {"code": promocode})
            promocode = cursor.fetchone()
            if promocode is None:
                return
            promocode_id = promocode[0]

            cursor.execute(
                "select * from app.subscriptions where profile_id = %(profile_id)s and tariff = %(tariff)s order by created_at desc limit 1",
                {
                    "profile_id": profile_id,
                    "tariff": tariff
                })
            subscription = cursor.fetchone()
            if subscription is None:
                return
            subscription_id = subscription[0]

            cursor.execute(
                "update app.subscriptions set promocode_id = %(promocode_id)s where id = %(subscription_id)s",
                {
                    "promocode_id": promocode_id,
                    "subscription_id": subscription_id
                })
