import json
from typing import Any, Iterable, Dict, List
from managed_portfolio.drivewealth.models import DriveWealthAccount, DriveWealthDocument, DriveWealthUser
from psycopg2.extras import RealDictCursor
from psycopg2 import sql
from gainy.data_access.repository import Repository
from gainy.utils import get_logger

logger = get_logger(__name__)


class DriveWealthRepository(Repository):

    def __init__(self, context_container):
        self.db_conn = context_container.db_conn

    #######################################################################################
    #TODO move to base repository
    def find_one(self, cls, filter_by: Dict[str, Any] = None):
        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(self._filter_query(cls, filter_by), filter_by)

            row = cursor.fetchone()

        return cls(row) if row else None

    def iterate_all(self,
                    cls,
                    filter_by: Dict[str, Any] = None) -> Iterable[Any]:
        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(self._filter_query(cls, filter_by), filter_by)

            for row in cursor:
                yield cls(row)

    def find_all(self, cls, filter_by: Dict[str, Any] = None) -> List[Any]:
        return list(self.iterate_all(cls, filter_by))

    def _filter_query(self, cls, filter_by):
        query = sql.SQL("SELECT * FROM {schema_name}.{table_name}").format(
            schema_name=sql.Identifier(cls.schema_name),
            table_name=sql.Identifier(cls.table_name))

        if filter_by:
            query += self._where_clause_statement(filter_by)

        return query

    @staticmethod
    def _where_clause_statement(filter_by: Dict[str, Any]):
        condition = sql.SQL(" AND ").join([
            sql.SQL(f"{{field}} = %({field})s").format(
                field=sql.Identifier(field)) for field in filter_by.keys()
        ])
        return sql.SQL(" WHERE ") + condition

    #######################################################################################

    def get_user(self, profile_id) -> DriveWealthUser:
        return self.find_one(DriveWealthUser, {"profile_id": profile_id})

    def upsert_user(self, profile_id, data) -> DriveWealthUser:
        entity = DriveWealthUser()
        entity.ref_id = data["id"]
        entity.profile_id = profile_id
        entity.status = data["status"]["name"]
        entity.data = json.dumps(data)

        self.persist(self.db_conn, entity)
        return entity

    def get_user_accounts(self,
                          drivewealth_user_id) -> Iterable[DriveWealthAccount]:
        return self.find_all(DriveWealthAccount,
                             {"drivewealth_user_id": drivewealth_user_id})

    def upsert_user_account(self, drivewealth_user_id,
                            data) -> DriveWealthAccount:
        entity = DriveWealthAccount()
        entity.ref_id = data["id"]
        entity.drivewealth_user_id = drivewealth_user_id
        #             entity.trading_account_id = data["trading_account_id"]
        entity.status = data["status"]['name']
        entity.ref_no = data["accountNo"]
        entity.nickname = data["nickname"]
        entity.cash_available_for_trade = data["bod"].get(
            "cashAvailableForTrading", 0)
        entity.cash_available_for_withdrawal = data["bod"].get(
            "cashAvailableForWithdrawal", 0)
        entity.cash_balance = data["bod"].get("cashBalance", 0)
        entity.data = json.dumps(data)

        self.persist(self.db_conn, entity)

        return entity

    def upsert_kyc_document(self, kyc_document_id: int,
                            data) -> DriveWealthDocument:
        id = data.get("id") or data.get("documentID")
        if not id:
            logger.error('Document without id', extra={"data": data})
            raise Exception('Document without id')

        entity = DriveWealthDocument()
        entity.ref_id = id
        entity.status = data["status"]["name"]
        entity.data = json.dumps(data)

        if kyc_document_id:
            entity.kyc_document_id = kyc_document_id

        self.persist(self.db_conn, entity)

        return entity
