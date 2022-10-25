from decimal import Decimal
import os

from trading.models import KycDocument
from trading.drivewealth.repository import DriveWealthRepository
from trading.drivewealth.models import DriveWealthBankAccount, DriveWealthPortfolio, DriveWealthFund, DriveWealthKycStatus

from gainy.utils import get_logger, env
from gainy.trading.drivewealth import DriveWealthApi as GainyDriveWealthApi
from gainy.trading.drivewealth.models import DriveWealthAccount

logger = get_logger(__name__)

DRIVEWEALTH_WLP_ID = os.getenv("DRIVEWEALTH_WLP_ID")
DRIVEWEALTH_PARENT_IBID = os.getenv("DRIVEWEALTH_PARENT_IBID")
DRIVEWEALTH_RIA_ID = os.getenv("DRIVEWEALTH_RIA_ID")
DRIVEWEALTH_RIA_PRODUCT_ID = os.getenv("DRIVEWEALTH_RIA_PRODUCT_ID")


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

    def create_portfolio(self, name, client_portfolio_id, description):
        return self._make_request(
            "POST", "/managed/portfolios", {
                'userID': DRIVEWEALTH_RIA_ID,
                'name': name,
                'clientPortfolioID': client_portfolio_id,
                'description': description,
                'holdings': [{
                    "type": "CASH_RESERVE",
                    "target": 1
                }],
            })

    def get_portfolio(self, portfolio: DriveWealthPortfolio):
        return self._make_request("GET",
                                  f"/managed/portfolios/{portfolio.ref_id}")

    def get_portfolio_status(self, portfolio: DriveWealthPortfolio):
        return self._make_request(
            "GET", f"/accounts/{portfolio.drivewealth_account_id}/portfolio")

    def get_instrument_details(self, ref_id: str = None, symbol: str = None):
        if ref_id:
            return self._make_request("GET", f"/instruments/{ref_id}")
        if symbol:
            return self._make_request("GET", f"/instruments/{symbol}")

        raise Exception('Either ref_id or symbol must be specified.')

    def create_fund(self, name, client_fund_id, description, holdings):
        return self._make_request(
            "POST", "/managed/funds", {
                'userID': DRIVEWEALTH_RIA_ID,
                'name': name,
                'clientFundID': client_fund_id,
                'description': description,
                'holdings': holdings,
            })

    def update_fund(self, fund: DriveWealthFund):
        data = self._make_request("PATCH", f"/managed/funds/{fund.ref_id}", {
            'holdings': fund.holdings,
        })
        fund.set_from_response(data)

    def update_portfolio(self, portfolio: DriveWealthPortfolio):
        # noinspection PyTypeChecker
        holdings = [{
            "type": "CASH_RESERVE",
            "target": portfolio.cash_target_weight,
        }] + [{
            "type": "FUND",
            "id": fund_id,
            "target": weight,
        } for fund_id, weight in portfolio.holdings.items()]

        data = self._make_request("PATCH",
                                  f"/managed/portfolios/{portfolio.ref_id}", {
                                      'holdings': holdings,
                                  })
        portfolio.set_from_response(data)

    def create_autopilot_run(self, account_ids: list):
        return self._make_request(
            "POST", f"/managed/autopilot/{DRIVEWEALTH_RIA_ID}", {
                'reviewOnly': False,
                'forceRebalance': True,
                'subAccounts': account_ids,
            })

    def get_autopilot_run(self, ref_id):
        return self._make_request("GET", f"/managed/autopilot/{ref_id}")

    def add_money(self, account_id, amount):
        return self._make_request(
            "POST", f"/accounts/{account_id}/transactions", {
                "comment": "Initial deposit",
                "amount": amount,
                "wlpFinTranTypeID": "00cec36e-4d83-4703-a769-894198b829f2",
                "source": "HUMAN",
                "batch": False
            })
