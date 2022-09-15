import json
from typing import Any, Iterable, Dict, List, Optional
from trading.drivewealth.models import DriveWealthAccount, DriveWealthDocument, DriveWealthUser, DriveWealthBankAccount, \
    DriveWealthFund, DriveWealthPortfolio, DriveWealthAuthToken, DriveWealthDeposit
from psycopg2.extras import RealDictCursor
from psycopg2 import sql
from gainy.data_access.repository import Repository
from gainy.utils import get_logger

logger = get_logger(__name__)


class DriveWealthRepository(Repository):

    def get_user(self, profile_id) -> DriveWealthUser:
        return self.find_one(DriveWealthUser, {"profile_id": profile_id})

    def upsert_user(self, profile_id, data) -> DriveWealthUser:
        entity = DriveWealthUser()
        entity.ref_id = data["id"]
        entity.profile_id = profile_id
        entity.status = data["status"]["name"]
        entity.data = json.dumps(data)

        self.persist(entity)
        return entity

    def get_user_accounts(self,
                          drivewealth_user_id) -> List[DriveWealthAccount]:
        return self.find_all(DriveWealthAccount,
                             {"drivewealth_user_id": drivewealth_user_id})

    def get_profile_fund(self, profile_id: int,
                         collection_id) -> DriveWealthFund:
        return self.find_one(DriveWealthFund, {
            "profile_id": profile_id,
            "collection_id": collection_id,
        })

    def get_profile_portfolio(self, profile_id: int) -> DriveWealthPortfolio:
        return self.find_one(DriveWealthPortfolio, {"profile_id": profile_id})

    def upsert_user_account(self, drivewealth_user_id,
                            data) -> DriveWealthAccount:
        entity = DriveWealthAccount()
        entity.drivewealth_user_id = drivewealth_user_id
        entity.set_from_response(data)

        self.persist(entity)

        return entity

    def upsert_kyc_document(self, kyc_document_id: Optional[int],
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

        self.persist(entity)

        return entity

    def get_latest_auth_token(self):
        return self.find_one(DriveWealthAuthToken, None, [("version", "desc")])
