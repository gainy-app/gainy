import json
from managed_portfolio.models import KycDocument
from common.context_container import ContextContainer
from psycopg2.extras import RealDictCursor


class ManagedPortfolioRepository:

    def __init__(self, context_container: ContextContainer):
        self.db_conn = context_container.db_conn

    def get_kyc_form(self, profile_id):
        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "select * from app.kyc_form where profile_id = %(profile_id)s",
                {
                    "profile_id": profile_id,
                })
            return cursor.fetchone()

    def update_kyc_form(self, profile_id: int, status: str) -> dict:
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                "update app.kyc_form set status = %(status)s where profile_id = %(profile_id)s",
                {
                    "profile_id": profile_id,
                    "status": status,
                })

    def upsert_kyc_document(self, profile_id: int,
                            document: KycDocument) -> dict:
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                """insert into app.kyc_documents (profile_id, uploaded_file_id, content_type, type, side)
                values (%(profile_id)s, %(uploaded_file_id)s, %(content_type)s, %(type)s, %(side)s)
                on conflict (uploaded_file_id) do update
                    set type = excluded.type,
                        side = excluded.side
                returning id""", {
                    "profile_id": profile_id,
                    "uploaded_file_id": document.uploaded_file_id,
                    "content_type": document.content_type,
                    "type": document.type,
                    "side": document.side,
                })

            document.id = cursor.fetchone()[0]
