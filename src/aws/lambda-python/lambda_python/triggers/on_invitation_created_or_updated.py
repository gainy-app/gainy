import datetime
import dateutil.relativedelta
import logging
import psycopg2
from services import BillingService
from common.hasura_function import HasuraTrigger

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class OnInvitationCreatedOrUpdated(HasuraTrigger):

    def __init__(self):
        super().__init__("on_invitation_created_or_updated")
        self.billing_service = BillingService()

    def get_allowed_profile_ids(self, op, data):
        payload = self._extract_payload(data)
        return [payload['to_profile_id']]

    def apply(self, db_conn, op, data):
        payload = self._extract_payload(data)
        invitation_id = payload['id']

        try:
            with db_conn.cursor() as cursor:
                cursor.execute(
                    """
                    insert into app.subscriptions(profile_id, invitation_id, is_promotion, period)
                    select from_profile_id, id, true, interval '1 month'
                    from app.invitations
                    where invitations.id = %(invitation_id)s""",
                    {"invitation_id": invitation_id})

            self.billing_service.recalculate_subscription_status(
                db_conn, payload['from_profile_id'])
        except psycopg2.errors.UniqueViolation as e:
            pass
