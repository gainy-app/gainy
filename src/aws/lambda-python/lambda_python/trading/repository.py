from psycopg2.extras import RealDictCursor

from gainy.trading.repository import TradingRepository as GainyTradingRepository
from gainy.trading.models import KycForm
from trading.models import KycDocument


class TradingRepository(GainyTradingRepository):

    def get_kyc_form(self, profile_id):
        # TODO modify to use entity
        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "select * from app.kyc_form where profile_id = %(profile_id)s",
                {
                    "profile_id": profile_id,
                })
            return cursor.fetchone()

    def remove_sensitive_kyc_data(self, profile_id: int):
        entity: KycForm = self.find_one(KycForm, {"profile_id": profile_id})
        if entity:
            entity.tax_id_value = None
            self.persist(entity)

        self.delete_by(KycDocument, {"profile_id": profile_id})
