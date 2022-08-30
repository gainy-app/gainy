import json
from psycopg2.extras import RealDictCursor


class DriveWealthRepository:

    def __init__(self, context_container):
        self.db_conn = context_container.db_conn

    def get_user(self, profile_id):
        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "select * from app.drivewealth_users where profile_id = %(profile_id)s",
                {
                    "profile_id": profile_id,
                })
            return cursor.fetchone()

    def upsert_user(self, profile_id, data) -> dict:
        data = {
            "ref_id": data["id"],
            "profile_id": profile_id,
            "status": data["status"]["name"],
            "data": json.dumps(data),
        }
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                '''insert into app.drivewealth_users(ref_id, profile_id, status, data)
                values (%(ref_id)s, %(profile_id)s, %(status)s, %(data)s)
                on conflict (ref_id) do update
                set status = excluded.status,
                    data = excluded.data''', data)

        return data
