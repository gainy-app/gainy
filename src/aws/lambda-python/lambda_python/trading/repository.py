from psycopg2.extras import RealDictCursor
from gainy.trading import TradingRepository as GainyTradingRepository


class TradingRepository(GainyTradingRepository):

    def get_kyc_form(self, profile_id):
        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "select * from app.kyc_form where profile_id = %(profile_id)s",
                {
                    "profile_id": profile_id,
                })
            return cursor.fetchone()

    def update_kyc_form(self, profile_id: int, status: str):
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                "update app.kyc_form set status = %(status)s where profile_id = %(profile_id)s",
                {
                    "profile_id": profile_id,
                    "status": status,
                })