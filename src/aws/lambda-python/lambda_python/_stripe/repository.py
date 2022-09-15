from psycopg2 import sql

from gainy.data_access.repository import Repository


class StripeRepository(Repository):

    def add_payment_intent(self, payment_intent_id, to_refund):
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                """insert into app.stripe_payments (payment_intent_ref_id, to_refund)
                values (%(payment_intent_id)s, %(to_refund)s)
                on conflict do nothing""", {
                    "payment_intent_id": payment_intent_id,
                    "to_refund": to_refund,
                })

    def update_payment_intent(self, payment_intent_id, data: dict):
        field_names = data.keys()

        set_clause = sql.SQL(',').join([
            sql.SQL("{field_name} = %({param_name})s").format(
                field_name=sql.Identifier(field_name),
                param_name=sql.SQL(field_name)) for field_name in field_names
        ])
        query = sql.SQL(
            "update app.stripe_payments set {set_clause} where payment_intent_ref_id = %(payment_intent_id)s"
        ).format(set_clause=set_clause)

        with self.db_conn.cursor() as cursor:
            cursor.execute(query, {
                **data,
                "payment_intent_id": payment_intent_id,
            })

    def is_to_refund(self, payment_intent_id):
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                "select to_refund from app.stripe_payments where payment_intent_ref_id = %(payment_intent_id)s",
                {
                    "payment_intent_id": payment_intent_id,
                })
            row = cursor.fetchone()

            return row and row[0]
