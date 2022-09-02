import os
import datetime
import dateutil
import requests
from managed_portfolio.models import KycDocument
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

    def get_user_accounts(self, user_id: str):
        return self._make_request("GET", f"/users/{user_id}/accounts")

    def link_bank_account(self, user_id: str, processor_token: str, name: str):
        return self._make_request(
            "POST", "/bank-accounts", {
                "plaidProcessorToken": processor_token,
                "userID": user_id,
                "bankAccountNickname": name,
            })

    def delete_bank_account(self, ref_id: str):
        return self._make_request("DELETE", f"/bank-accounts/{ref_id}")

    def _get_token(self):
        # TODO redis
        if self._token_data is not None and datetime.datetime.now(
                tz=datetime.timezone.utc) > self._token_data['expiresAt']:
            return self._token_data["authToken"]

        token_data = self._make_request(
            "POST", "/auth", {
                "appTypeID": 4,
                "username": DRIVEWEALTH_API_USERNAME,
                "password": DRIVEWEALTH_API_PASSWORD
            })

        token_data['expiresAt'] = dateutil.parser.parse(
            token_data['expiresAt'])
        self._token_data = token_data

        return token_data["authToken"]

    def _make_request(self, method, url, post_data=None):
        headers = {"dw-client-app-key": DRIVEWEALTH_APP_KEY}

        if url != "/auth":
            headers["dw-auth-token"] = self._get_token()

        response = requests.request(method,
                                    DRIVEWEALTH_API_URL + url,
                                    json=post_data,
                                    headers=headers)

        try:
            response_data = response.json()
        except:
            response_data = None

        logging_extra = {
            "post_data": post_data,
            "status_code": response.status_code,
            "response_data": response_data,
        }

        if response.status_code is None or response.status_code < 200 or response.status_code > 299:
            logger.error("[DRIVEWEALTH] %s %s" % (method, url),
                         extra=logging_extra)

            if response_data is not None and 'message' in response_data:
                raise ApiException(
                    "%s: %s" %
                    (response_data["errorCode"], response_data["message"]))
            else:
                raise ApiException("Failed: %d" % response.status_code)

        logger.info("[DRIVEWEALTH] %s %s" % (method, url), extra=logging_extra)

        return response_data
