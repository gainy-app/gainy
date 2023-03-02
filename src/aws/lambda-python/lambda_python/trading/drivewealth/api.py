from typing import List, Iterable

import datetime

from decimal import Decimal

from gainy.trading.drivewealth.config import DRIVEWEALTH_WLP_ID, DRIVEWEALTH_PARENT_IBID, DRIVEWEALTH_RIA_ID, \
    DRIVEWEALTH_RIA_PRODUCT_ID
from trading.models import KycDocument, TradingStatementType
from trading.drivewealth.repository import DriveWealthRepository
from trading.drivewealth.models import DriveWealthBankAccount, DriveWealthKycStatus, DriveWealthStatement

from gainy.utils import get_logger, env, ENV_PRODUCTION, DATE_ISO8601_FORMAT
from gainy.trading.drivewealth import DriveWealthApi as GainyDriveWealthApi
from gainy.trading.drivewealth.models import DriveWealthAccount, DriveWealthRedemption

logger = get_logger(__name__)


def _hydrate_documents(account: DriveWealthAccount, type: TradingStatementType,
                       data: list) -> Iterable[DriveWealthStatement]:
    for i in data:
        entity = DriveWealthStatement()
        entity.set_from_response(i)
        entity.type = type
        entity.account_id = account.ref_id
        entity.user_id = account.drivewealth_user_id
        yield entity


class DriveWealthApi(GainyDriveWealthApi):

    def __init__(self, repository: DriveWealthRepository):
        self.repository = repository

    def create_user(self, documents: list):
        return self._make_request(
            "POST", "/users", {
                "userType": "INDIVIDUAL_TRADER",
                "wlpID": DRIVEWEALTH_WLP_ID,
                "parentIBID": DRIVEWEALTH_PARENT_IBID,
                "documents": documents,
            })

    def create_account(self, user_id: str):
        params = {
            "userID": user_id,
            "accountType": "LIVE",
            "accountManagementType": "RIA_MANAGED",
            "tradingType": "CASH",
            "riaUserID": DRIVEWEALTH_RIA_ID,
            "riaProductID": DRIVEWEALTH_RIA_PRODUCT_ID,
        }
        if env() != ENV_PRODUCTION:
            params["ignoreMarketHoursForTest"] = True

        return self._make_request("POST", "/accounts", params)

    def upload_document(self, user_id: str, document: KycDocument,
                        file_base64):
        return self._make_request(
            "POST", "/documents", {
                "userID": user_id,
                "type": document.type,
                "document": file_base64,
                "side": document.side,
            })

    def update_user(self, user_id: str, documents: list):
        return self._make_request("PATCH", f"/users/{user_id}", {
            "documents": documents,
        })

    def get_user_documents(self, user_id: str):
        return self._make_request("GET", f"/users/{user_id}/documents")

    def get_kyc_status(self, user_id: str) -> DriveWealthKycStatus:
        return DriveWealthKycStatus(
            self._make_request("GET", f"/users/{user_id}/kyc-status"))

    def get_user_bank_accounts(self, user_id: str):
        return self._make_request("GET", f"/users/{user_id}/bank-accounts")

    def link_bank_account(self, user_id: str, processor_token: str, name: str):
        return self._make_request(
            "POST", "/bank-accounts", {
                "plaidProcessorToken": processor_token,
                "userID": user_id,
                "bankAccountNickname": name,
            })

    def delete_bank_account(self, ref_id: str):
        return self._make_request("DELETE", f"/bank-accounts/{ref_id}")

    def get_user_deposits(self, user_id):
        return self._make_request("GET", f"/users/{user_id}/funding/deposits")

    def get_deposit(self, deposit_id):
        return self._make_request("GET", f"/funding/deposits/{deposit_id}")

    def get_user_redemptions(self, user_id):
        return self._make_request("GET",
                                  f"/users/{user_id}/funding/redemptions")

    def get_redemption(self, redemption_id):
        return self._make_request("GET",
                                  f"/funding/redemptions/{redemption_id}")

    def create_deposit(self, amount: Decimal, account: DriveWealthAccount,
                       bank_account: DriveWealthBankAccount):
        return self._make_request(
            "POST", "/funding/deposits", {
                'accountNo': account.ref_no,
                'amount': amount,
                'currency': 'USD',
                'type': 'ACH',
                'bankAccountID': bank_account.ref_id,
            })

    def create_redemption(self, amount: Decimal, account, bank_account):
        return self._make_request(
            "POST", "/funding/redemptions", {
                'accountNo': account.ref_no,
                'amount': amount,
                'currency': 'USD',
                'type': 'ACH',
                'bankAccountID': bank_account.ref_id,
            })

    def update_redemption(self, redemption: DriveWealthRedemption,
                          status: str):
        data = self._make_request("PATCH",
                                  f"/funding/redemptions/{redemption.ref_id}",
                                  {
                                      'status': status,
                                      'statusComment': 'Updated by Gainy',
                                  })
        redemption.set_from_response(data)

    def get_autopilot_run(self, ref_id):
        return self._make_request("GET", f"/managed/autopilot/{ref_id}")

    def get_autopilot_runs(self) -> list:
        return self._make_request(
            "GET", f"/users/{DRIVEWEALTH_RIA_ID}/managed/autopilot")

    def add_money(self, account_id, amount):
        return self._make_request(
            "POST", f"/accounts/{account_id}/transactions", {
                "comment": "Initial deposit",
                "amount": amount,
                "wlpFinTranTypeID": "00cec36e-4d83-4703-a769-894198b829f2",
                "source": "HUMAN",
                "batch": False
            })

    def get_statement_url(self, statement: DriveWealthStatement) -> str:
        return self._make_request(
            "GET",
            f"/statements/{statement.account_id}/{statement.file_key}")["url"]

    def get_documents_trading_confirmations(
            self, account: DriveWealthAccount) -> List[DriveWealthStatement]:
        start_date = account.created_at.date()
        end_date = datetime.date.today() + datetime.timedelta(days=1)
        get_data = {
            "from": start_date.strftime(DATE_ISO8601_FORMAT),
            "to": end_date.strftime(DATE_ISO8601_FORMAT),
        }

        data = self._make_request("GET",
                                  f"/accounts/{account.ref_id}/confirms",
                                  get_data=get_data)

        return list(
            _hydrate_documents(account,
                               TradingStatementType.TRADE_CONFIRMATION, data))

    def get_documents_tax(
            self, account: DriveWealthAccount) -> List[DriveWealthStatement]:
        start_date = account.created_at.date()
        end_date = datetime.date.today() + datetime.timedelta(days=1)
        get_data = {
            "from": start_date.strftime(DATE_ISO8601_FORMAT),
            "to": end_date.strftime(DATE_ISO8601_FORMAT),
        }

        data = self._make_request("GET",
                                  f"/accounts/{account.ref_id}/taxforms",
                                  get_data=get_data)

        return list(_hydrate_documents(account, TradingStatementType.TAX,
                                       data))

    def get_documents_statements(
            self, account: DriveWealthAccount) -> List[DriveWealthStatement]:
        start_date = account.created_at.date()
        end_date = datetime.date.today() + datetime.timedelta(days=1)
        get_data = {
            "from": start_date.strftime(DATE_ISO8601_FORMAT),
            "to": end_date.strftime(DATE_ISO8601_FORMAT),
        }

        data = self._make_request("GET",
                                  f"/accounts/{account.ref_id}/statements",
                                  get_data=get_data)

        return list(
            _hydrate_documents(account, TradingStatementType.MONTHLY_STATEMENT,
                               data))

    def iterate_user_transactions(
            self, account: DriveWealthAccount) -> Iterable[dict]:
        start_date = account.created_at.date()
        now = datetime.date.today()
        while start_date <= now:
            end_date = start_date + datetime.timedelta(weeks=1)
            params = {
                "from": start_date.strftime(DATE_ISO8601_FORMAT),
                "to": end_date.strftime(DATE_ISO8601_FORMAT),
            }

            yield from self._make_request(
                "GET",
                f"/accounts/{account.ref_id}/transactions",
                get_data=params)

            start_date = end_date

    def get_order(self, order_id):
        return self._make_request("GET", f"/orders/{order_id}")
