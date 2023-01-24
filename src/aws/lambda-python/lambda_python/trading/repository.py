from psycopg2.extras import RealDictCursor

from gainy.exceptions import NotFoundException
from gainy.trading.repository import TradingRepository as GainyTradingRepository
from gainy.trading.models import TradingAccount
from trading.models import KycDocument, ProfileKycStatus, KycForm


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

    def update_kyc_form(self, profile_id: int, status: str):
        entity: KycForm = self.find_one(KycForm, {"profile_id": profile_id})
        if not entity:
            return

        entity.status = status
        self.persist(entity)

    def remove_sensitive_kyc_data(self, profile_id: int):
        entity: KycForm = self.find_one(KycForm, {"profile_id": profile_id})
        if entity:
            entity.tax_id_value = None
            self.persist(entity)

        self.delete_by(KycDocument, {"profile_id": profile_id})

    def get_trading_account(self, profile_id: int) -> TradingAccount:
        trading_account = self.find_one(TradingAccount,
                                        {"profile_id": profile_id})

        if not trading_account:
            raise NotFoundException()

        return trading_account

    def get_actual_kyc_status(self, profile_id: int) -> ProfileKycStatus:
        status = self.find_one(ProfileKycStatus, {"profile_id": profile_id},
                               order_by=[("created_at", "desc")])
        if status:
            return status

        raise NotFoundException()
