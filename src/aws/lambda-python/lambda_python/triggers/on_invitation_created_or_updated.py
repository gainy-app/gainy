import datetime
import dateutil.relativedelta
import logging
import psycopg2
from common.hasura_function import HasuraTrigger

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class OnInvitationCreatedOrUpdated(HasuraTrigger):

    def __init__(self):
        super().__init__("on_invitation_created_or_updated")

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
                cursor.execute(
                    """
                    with profile_subscription_end_date as
                             (
                                 select profile_id,
                                        max(end_date) as end_date
                                 from (
                                          select profile_id,
                                                 created_at +
                                                 sum(period) over (partition by profile_id order by created_at desc) as end_date
                                          from app.subscriptions
                                      ) t
                                 group by profile_id
                             )
                    update app.profiles
                    set subscription_end_date = profile_subscription_end_date.end_date
                    from profile_subscription_end_date
                    where profiles.id = profile_subscription_end_date.profile_id
                      and profiles.id = %(profile_id)s""", {
                        "profile_id": payload['from_profile_id'],
                    })
        except psycopg2.errors.UniqueViolation as e:
            pass
