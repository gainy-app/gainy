import json
from typing import Any, Iterable, Dict, List
from trading.drivewealth.models import DriveWealthAccount, DriveWealthDocument, DriveWealthUser, DriveWealthBankAccount, DriveWealthFund, DriveWealthPortfolio
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

    def get_user_fund(self, user: DriveWealthUser,
                      collection_id) -> DriveWealthFund:
        return self.find_one(DriveWealthFund, {
            "drivewealth_user_id": user.ref_id,
            "collection_id": collection_id
        })

    def get_user_portfolio(self,
                           user: DriveWealthUser) -> DriveWealthPortfolio:
        return self.find_one(DriveWealthPortfolio,
                             {"drivewealth_user_id": user.ref_id})

    def upsert_user_account(self, drivewealth_user_id,
                            data) -> DriveWealthAccount:
        entity = DriveWealthAccount()
        entity.ref_id = data["id"]
        entity.drivewealth_user_id = drivewealth_user_id
        entity.status = data["status"]['name']
        entity.ref_no = data["accountNo"]
        entity.nickname = data["nickname"]
        entity.cash_available_for_trade = data["bod"].get(
            "cashAvailableForTrading", 0)
        entity.cash_available_for_withdrawal = data["bod"].get(
            "cashAvailableForWithdrawal", 0)
        entity.cash_balance = data["bod"].get("cashBalance", 0)
        entity.data = json.dumps(data)

        self.persist(entity)

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

        self.persist(entity)

        return entity

    def upsert_bank_account(
            self,
            data,
            plaid_access_token_id: int = None,
            plaid_account_id: str = None) -> DriveWealthBankAccount:
        entity = DriveWealthBankAccount()
        entity.ref_id = data['id']
        entity.drivewealth_user_id = data["userDetails"]['userID']
        entity.status = data["status"]
        entity.bank_account_nickname = data["bankAccountDetails"][
            'bankAccountNickname']
        entity.bank_account_number = data["bankAccountDetails"][
            'bankAccountNumber']
        entity.bank_routing_number = data["bankAccountDetails"][
            'bankRoutingNumber']
        entity.holder_name = " ".join([
            data["userDetails"]['firstName'], data["userDetails"]['lastName']
        ])
        entity.bank_account_type = data["bankAccountDetails"].get(
            'bankAccountType')
        if plaid_access_token_id:
            entity.plaid_access_token_id = plaid_access_token_id
        if plaid_account_id:
            entity.plaid_account_id = plaid_account_id
        entity.data = json.dumps(data)

        self.persist(entity)

        return entity
