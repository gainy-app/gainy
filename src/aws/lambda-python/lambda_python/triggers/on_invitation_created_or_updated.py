import datetime
import dateutil.relativedelta
import psycopg2
from services import BillingService
from common.context_container import ContextContainer
from common.hasura_function import HasuraTrigger
from gainy.utils import get_logger

logger = get_logger(__name__)


class OnInvitationCreatedOrUpdated(HasuraTrigger):

    def __init__(self):
        super().__init__("on_invitation_created_or_updated")
        self.billing_service = BillingService()

    def get_allowed_profile_ids(self, op, data):
        payload = self._extract_payload(data)
        return [payload['to_profile_id']]

    def apply(self, op, data, context_container: ContextContainer):
        db_conn = context_container.db_conn
        payload = self._extract_payload(data)
        invitation_id = payload['id']

        try:
            with db_conn.cursor() as cursor:
                cursor.execute(
                    """
                    insert into app.subscriptions(profile_id, invitation_id, is_promotion, period)
                    select from_profile_id, id, true, interval '1 month'
                    from app.invitations
                    where invitations.id = %(invitation_id)s
                    union all
                    select to_profile_id, id, true, interval '1 month'
                    from app.invitations
                    where invitations.id = %(invitation_id)s""",
                    {"invitation_id": invitation_id})

            self.billing_service.recalculate_subscription_status(
                db_conn, payload['from_profile_id'])
            self.billing_service.recalculate_subscription_status(
                db_conn, payload['to_profile_id'])
        except psycopg2.errors.UniqueViolation as e:
            pass
