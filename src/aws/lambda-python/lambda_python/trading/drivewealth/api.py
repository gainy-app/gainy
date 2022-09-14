from decimal import Decimal
import os
import requests

from gainy.data_access.db_lock import LockAcquisitionTimeout
from trading.drivewealth.locking_functions import UpdateDriveWealthAuthToken
from trading.drivewealth.repository import DriveWealthRepository
from trading.models import KycDocument
from trading.drivewealth.models import DriveWealthAccount, DriveWealthBankAccount, DriveWealthPortfolio, \
    DriveWealthFund, DriveWealthAuthToken, DriveWealthKycStatus
from common.exceptions import ApiException
from gainy.utils import get_logger, env

logger = get_logger(__name__)

DRIVEWEALTH_APP_KEY = os.getenv("DRIVEWEALTH_APP_KEY")
DRIVEWEALTH_WLP_ID = os.getenv("DRIVEWEALTH_WLP_ID")
DRIVEWEALTH_PARENT_IBID = os.getenv("DRIVEWEALTH_PARENT_IBID")
DRIVEWEALTH_RIA_ID = os.getenv("DRIVEWEALTH_RIA_ID")
DRIVEWEALTH_RIA_PRODUCT_ID = os.getenv("DRIVEWEALTH_RIA_PRODUCT_ID")
DRIVEWEALTH_API_USERNAME = os.getenv("DRIVEWEALTH_API_USERNAME")
DRIVEWEALTH_API_PASSWORD = os.getenv("DRIVEWEALTH_API_PASSWORD")
DRIVEWEALTH_API_URL = os.getenv("DRIVEWEALTH_API_URL")


class DriveWealthApi:
    _token_data = None

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
        return self._make_request(
            "POST", "/accounts", {
                "userID": user_id,
                "accountType": "LIVE",
                "accountManagementType": "RIA_MANAGED",
                "tradingType": "CASH",
                "ignoreMarketHoursForTest": env() != "production",
                "riaUserID": DRIVEWEALTH_RIA_ID,
                "riaProductID": DRIVEWEALTH_RIA_PRODUCT_ID,
            })

    def update_account(self, account_ref_id, portfolio_ref_id):
        return self._make_request("PATCH", f"/accounts/{account_ref_id}",
                                  {"ria": {
                                      "portfolioID": portfolio_ref_id
                                  }})

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

    def get_user(self, user_id: str):
        return self._make_request("GET", f"/users/{user_id}")

    def get_user_documents(self, user_id: str):
        return self._make_request("GET", f"/users/{user_id}/documents")

    def get_kyc_status(self, user_id: str) -> DriveWealthKycStatus:
        return DriveWealthKycStatus(
            self._make_request("GET", f"/users/{user_id}/kyc-status"))

    def get_user_accounts(self, user_id: str):
        return self._make_request("GET", f"/users/{user_id}/accounts")

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

    def create_deposit(self, amount: Decimal, account: DriveWealthAccount,
                       bank_account: DriveWealthBankAccount):
        return self._make_request(
            "POST", "/funding/deposits", {
                'accountNo': account.ref_no,
                'amount': amount,
                'currency': 'USD',
                'type': 'INSTANT_FUNDING',
                'details': {
                    "accountHolderName": bank_account.holder_name,
                    "bankAccountType": bank_account.bank_account_type,
                    "bankAccountNumber": bank_account.bank_account_number,
                    "bankRoutingNumber": bank_account.bank_routing_number,
                    "country": "USA"
                },
            })

    def create_redemption(self, amount: Decimal, account, bank_account):
        return self._make_request(
            "POST", "/funding/redemptions", {
                'accountNo': account.ref_no,
                'amount': amount,
                'currency': 'USD',
                'type': 'ACH_MANUAL',
                'details': {
                    "beneficiaryName": bank_account.holder_name,
                    "bankAccountType": bank_account.bank_account_type,
                    "bankAccountNumber": bank_account.bank_account_number,
                    "bankRoutingNumber": bank_account.bank_routing_number
                },
            })

    def create_portfolio(self, user_id, name, client_portfolio_id,
                         description):
        return self._make_request(
            "POST", "/managed/portfolios", {
                'userID': user_id,
                'name': name,
                'clientPortfolioID': client_portfolio_id,
                'description': description,
                'holdings': [{
                    "type": "CASH_RESERVE",
                    "target": 1
                }],
            })

    def get_portfolio_status(self, portfolio: DriveWealthPortfolio):
        return self._make_request(
            "GET", f"/accounts/{portfolio.drivewealth_account_id}/portfolio")

    def get_instrument_details(self, symbol: str):
        return self._make_request("GET", f"/instruments/{symbol}")

    def create_fund(self, user_id, name, client_fund_id, description,
                    holdings):
        return self._make_request(
            "POST", "/managed/funds", {
                'userID': user_id,
                'name': name,
                'clientFundID': client_fund_id,
                'description': description,
                'holdings': holdings,
            })

    def update_fund(self, fund: DriveWealthFund):
        return self._make_request("PATCH", f"/managed/funds/{fund.ref_id}", {
            'holdings': fund.holdings,
        })

    def update_portfolio(self, portfolio: DriveWealthPortfolio):
        return self._make_request("PATCH",
                                  f"/managed/portfolios/{portfolio.ref_id}", {
                                      'holdings': portfolio.holdings,
                                  })

    def create_autopilot_run(self, account_ids):
        return self._make_request(
            "POST", f"/autopilot/{DRIVEWEALTH_RIA_ID}", {
                'reviewOnly': False,
                'forceRebalance': True,
                'subAccounts': account_ids,
            })

    def add_money(self, account_id, amount):
        return self._make_request(
            "POST", f"/accounts/{account_id}/transactions", {
                "comment": "Initial deposit",
                "amount": amount,
                "wlpFinTranTypeID": "00cec36e-4d83-4703-a769-894198b829f2",
                "source": "HUMAN",
                "batch": False
            })

    def get_auth_token(self):
        return self._make_request(
            "POST", "/auth", {
                "appTypeID": 4,
                "username": DRIVEWEALTH_API_USERNAME,
                "password": DRIVEWEALTH_API_PASSWORD
            })

    def _get_token(self, force_token_refresh: bool = False):
        token = self.repository.get_latest_auth_token()

        if force_token_refresh or not token or token.is_expired():
            token = self._refresh_token(force_token_refresh)

        return token.auth_token

    def _refresh_token(self, force: bool) -> DriveWealthAuthToken:
        func = UpdateDriveWealthAuthToken(self.repository, self, force)
        try:
            return func.execute()
        except LockAcquisitionTimeout as e:
            logger.exception(e)
            raise e

    def _make_request(self,
                      method,
                      url,
                      post_data=None,
                      force_token_refresh=False):
        headers = {"dw-client-app-key": DRIVEWEALTH_APP_KEY}

        if url != "/auth":
            headers["dw-auth-token"] = self._get_token(force_token_refresh)

        response = requests.request(method,
                                    DRIVEWEALTH_API_URL + url,
                                    json=post_data,
                                    headers=headers)

        try:
            response_data = response.json()
        except:
            response_data = None

        status_code = response.status_code
        logging_extra = {
            "post_data": post_data,
            "status_code": status_code,
            "response_data": response_data,
            "requestId": response.headers.get("dw-request-id"),
        }

        if status_code is None or status_code < 200 or status_code > 299:
            if status_code == 401 and response_data.get(
                    "errorCode") == "L075" and not force_token_refresh:
                logger.info('[DRIVEWEALTH] token expired', extra=logging_extra)
                return self._make_request(method, url, post_data, True)

            logger.error("[DRIVEWEALTH] %s %s" % (method, url),
                         extra=logging_extra)

            if response_data is not None and 'message' in response_data:
                raise ApiException(
                    "%s: %s" %
                    (response_data["errorCode"], response_data["message"]))
            else:
                raise ApiException("Failed: %d" % status_code)

        logger.info("[DRIVEWEALTH] %s %s" % (method, url), extra=logging_extra)

        return response_data
