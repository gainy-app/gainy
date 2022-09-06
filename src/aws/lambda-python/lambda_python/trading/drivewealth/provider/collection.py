from decimal import Decimal
from typing import Dict, Any, List
from trading.exceptions import InsufficientFundsException
from trading.models import TradingCollectionVersion
from trading.drivewealth.models import DriveWealthAccount, DriveWealthFund, DriveWealthPortfolio, \
    DriveWealthPortfolioStatus, DriveWealthAutopilotRun, PRECISION
from trading.drivewealth.api import DriveWealthApi
from trading.drivewealth.repository import DriveWealthRepository
from gainy.utils import get_logger

logger = get_logger(__name__)


class DriveWealthProviderCollection:
    drivewealth_repository: DriveWealthRepository = None
    api: DriveWealthApi = None

    def __init__(self, drivewealth_repository: DriveWealthRepository,
                 api: DriveWealthApi):
        self.drivewealth_repository = drivewealth_repository
        self.api = api

    def reconfigure_collection_holdings(
            self, collection_version: TradingCollectionVersion):
        user = self.drivewealth_repository.get_user(
            collection_version.profile_id)
        account = self.drivewealth_repository.get_user_accounts(user.ref_id)[0]
        profile_id = collection_version.profile_id
        portfolio = self._upsert_portfolio(profile_id, account)
        chosen_fund = self._upsert_fund(profile_id, collection_version)

        self._handle_cash_amount_change(collection_version.target_amount_delta,
                                        portfolio, chosen_fund)

        self.api.update_portfolio(portfolio)
        self.api.update_fund(chosen_fund)
        self._create_autopilot_run(account)

    def _create_autopilot_run(self, account: DriveWealthAccount):
        data = self.api.create_autopilot_run(account)
        entity = DriveWealthAutopilotRun(data)
        entity.accounts = [account.ref_id]
        self.drivewealth_repository.persist(entity)
        return entity

    def _upsert_portfolio(self, profile_id, account: DriveWealthAccount):
        repository = self.drivewealth_repository
        user = repository.get_user(profile_id)
        portfolio = repository.get_user_portfolio(user)

        if not portfolio:
            user_id = user.ref_id
            name = f"Gainy {user_id}'s portfolio"
            client_portfolio_id = profile_id
            description = name

            data = self.api.create_portfolio(user_id, name,
                                             client_portfolio_id, description)
            portfolio = DriveWealthPortfolio(data)
            repository.persist(portfolio)

        if not portfolio.drivewealth_account_id:
            self.api.update_account(account.ref_id, portfolio.ref_id)
            portfolio.drivewealth_account_id = account.ref_id
            repository.persist(portfolio)

        return portfolio

    def _upsert_fund(
            self, profile_id,
            collection_version: TradingCollectionVersion) -> DriveWealthFund:
        collection_id = collection_version.collection_id
        weights = collection_version.weights
        repository = self.drivewealth_repository
        user = repository.get_user(profile_id)
        fund = repository.get_user_fund(user, collection_id)
        new_fund_holdings = self._generate_new_fund_holdings(fund, weights)

        if fund:
            fund.holdings = new_fund_holdings
            data = self.api.update_fund(fund)
            fund.data = data
        else:
            user_id = user.ref_id
            name = f"Gainy {user_id}'s fund for collection {collection_id}"
            client_fund_id = f"{profile_id}_{collection_id}"
            description = name

            data = self.api.create_fund(user_id, name, client_fund_id,
                                        description, new_fund_holdings)

            fund = DriveWealthFund(data)

        fund.weights = weights
        fund.collection_id = collection_id
        fund.trading_collection_version_id = collection_version.id
        repository.persist(fund)

        return fund

    def _generate_new_fund_holdings(
            self, fund: DriveWealthFund,
            weights: Dict[str, Any]) -> List[Dict[str, Any]]:
        new_holdings = {}

        # add old holdings with zero weight for the api to remove it if they are missing from the weights
        if fund:
            for holding in fund.holdings:
                new_holdings[holding["instrumentID"]] = 0

        # TODO check dw instruments symbols to match our symbols
        for symbol, weight in weights.items():
            instrument = self.api.get_instrument_details(symbol)
            new_holdings[instrument["id"]] = weight

        return [{
            "instrumentID": k,
            "target": i,
        } for k, i in new_holdings.items()]

    def _handle_cash_amount_change(self, target_amount_delta: Decimal,
                                   portfolio: DriveWealthPortfolio,
                                   chosen_fund: DriveWealthFund):
        if not target_amount_delta:
            return 0

        portfolio_status = self._update_portfolio(portfolio)
        cash_value = portfolio_status.cash_value
        cash_actual_weight = portfolio_status.cash_actual_weight
        fund_value = portfolio_status.get_fund_value(chosen_fund.ref_id)
        fund_actual_weight = portfolio_status.get_fund_actual_weight(
            chosen_fund.ref_id)

        logging_extra = {
            "target_amount_delta": target_amount_delta,
            "portfolio_status": portfolio_status.to_dict(),
            "portfolio": portfolio.to_dict(),
        }
        logger.info('_handle_cash_amount_change step0', extra=logging_extra)

        # TODO handle initial buy after deposit?
        if target_amount_delta > 0:
            if target_amount_delta - cash_value > PRECISION:
                raise InsufficientFundsException()
            weight_delta = target_amount_delta / cash_value * cash_actual_weight
        else:
            if abs(target_amount_delta) - fund_value > PRECISION:
                raise InsufficientFundsException()
            weight_delta = target_amount_delta / fund_value * fund_actual_weight

        portfolio.set_target_weights_from_status_actual_weights(
            portfolio_status)
        logging_extra["weight_delta"] = weight_delta
        logging_extra["portfolio"] = portfolio.to_dict()
        logger.info('_handle_cash_amount_change step1', extra=logging_extra)

        portfolio.move_cash_to_fund(chosen_fund, weight_delta)
        self.drivewealth_repository.persist(portfolio)
        logging_extra["portfolio"] = portfolio.to_dict()
        logger.info('_handle_cash_amount_change step2', extra=logging_extra)

    def _on_money_transfer(self, profile_id):
        repository = self.drivewealth_repository
        user = repository.get_user(profile_id)
        portfolio = repository.get_user_portfolio(user)
        if not portfolio:
            return

        self._update_portfolio(portfolio)

    def _update_portfolio(
            self,
            portfolio: DriveWealthPortfolio) -> DriveWealthPortfolioStatus:
        data = self.api.get_portfolio_status(portfolio)
        portfolio_status = DriveWealthPortfolioStatus(data)
        self.drivewealth_repository.persist(portfolio_status)
        return portfolio_status
