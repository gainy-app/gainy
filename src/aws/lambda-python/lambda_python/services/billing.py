import os
from services.logging import get_logger


class BillingService:

    def recalculate_subscription_status(self, db_conn, profile_id):
        with db_conn.cursor() as cursor:
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
                                      where subscriptions.expired_at is null

                                      union all

                                      select profiles.id,
                                             null as end_date
                                      from app.profiles
                                               left join app.subscriptions on subscriptions.profile_id = profiles.id
                                      where profiles.subscription_end_date is not null
                                        and (subscriptions.id is null or subscriptions.expired_at is not null)
                                  ) t
                             group by profile_id
                         )
                update app.profiles
                set subscription_end_date = profile_subscription_end_date.end_date
                from profile_subscription_end_date
                where profiles.id = profile_subscription_end_date.profile_id
                  and profiles.id = %(profile_id)s""", {
                    "profile_id": profile_id,
                })
