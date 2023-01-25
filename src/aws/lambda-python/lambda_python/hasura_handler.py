import os

from gainy.utils import setup_exception_logger_hook, setup_lambda_logging_middleware, get_logger

from common.hasura_dispatcher import HasuraActionDispatcher, HasuraTriggerDispatcher
from recommendation.match_score_action import GetMatchScoreByCollection, GetMatchScoreByTicker, \
    GetMatchScoreByTickerList
from recommendation.recommendation_action import GetRecommendedCollections
from portfolio.plaid.actions import *
from portfolio.actions import *
from portfolio.triggers import *
from search.algolia_search import SearchTickers, SearchCollections
from search.news_search import SearchNews
from triggers import *
from actions import *
from trading.actions import *
from _stripe.actions import StripeGetCheckoutUrl, StripeGetPaymentSheetData, StripeWebhook
from _stripe.triggers import StripeDeletePaymentMethod

setup_exception_logger_hook()

ENV = os.environ['ENV']

ALGOLIA_APP_ID = os.getenv("ALGOLIA_APP_ID")
ALGOLIA_TICKERS_INDEX = os.getenv("ALGOLIA_TICKERS_INDEX")
ALGOLIA_COLLECTIONS_INDEX = os.getenv("ALGOLIA_COLLECTIONS_INDEX")
ALGOLIA_SEARCH_API_KEY = os.getenv("ALGOLIA_SEARCH_API_KEY")

GNEWS_API_TOKEN = os.getenv("GNEWS_API_TOKEN")

API_GATEWAY_PROXY_INTEGRATION = os.getenv(
    "AWS_LAMBDA_API_GATEWAY_PROXY_INTEGRATION", "True") == "True"

ACTIONS = [
    SetRecommendationSettings(),
    GetRecommendedCollections(),
    GetMatchScoreByTicker(),
    GetMatchScoreByTickerList(),
    GetMatchScoreByCollection(),
    ApplyPromocode(),
    UpdatePurchases(),
    GetPromocode(),
    GetPreSignedUploadForm(),
    SendAppLink(),

    # Portfolio
    CreatePlaidLinkToken(),
    LinkPlaidAccount(),
    GetPortfolioHoldings(),
    GetPortfolioTransactions(),
    GetPortfolioChart(),
    GetPortfolioChartPreviousPeriodClose(),
    GetPortfolioPieChart(),
    PlaidWebhook(),

    # Stripe
    StripeGetCheckoutUrl(),
    StripeGetPaymentSheetData(),
    StripeWebhook(),

    # Search
    SearchTickers(ALGOLIA_APP_ID, ALGOLIA_SEARCH_API_KEY,
                  ALGOLIA_TICKERS_INDEX),
    SearchCollections(ALGOLIA_APP_ID, ALGOLIA_SEARCH_API_KEY,
                      ALGOLIA_COLLECTIONS_INDEX),
    SearchNews(GNEWS_API_TOKEN),

    # KYC
    KycGetFormConfig(),
    KycGetStatus(),
    KycValidateAddress(),
    KycSendForm(),
    KycAddDocument(),

    # Verification
    VerificationSendCode(),
    VerificationVerifyCode(),

    # Trading
    TradingLinkBankAccountWithPlaid(),
    TradingGetFundingAccounts(),
    TradingDeleteFundingAccount(),
    TradingDepositFunds(),
    TradingWithdrawFunds(),
    TradingReconfigureCollectionHoldings(),
    TradingGetActualCollectionHoldings(),
    TradingSyncProviderData(),
    TradingCancelPendingOrder(),
    TradingDownloadDocument(),
    TradingCreateStockOrder(),

    # Debug
    TradingAddMoney(),
    TradingDeleteData(),
    ReHandleQueueMessages(),
]

action_dispatcher = HasuraActionDispatcher(ACTIONS,
                                           API_GATEWAY_PROXY_INTEGRATION)


def handle_action(event, context):
    setup_lambda_logging_middleware(context)
    logger = get_logger(__name__)
    logger.info('handle_action', extra={"event": event})
    return action_dispatcher.handle(event, context)


TRIGGERS = [
    SetUserCategories(),
    OnUserCreated(ENV),
    SetRecommendations(),
    OnPlaidAccessTokenCreated(),
    OnInvitationCreatedOrUpdated(),
    StripeDeletePaymentMethod(),
]

trigger_dispatcher = HasuraTriggerDispatcher(TRIGGERS,
                                             API_GATEWAY_PROXY_INTEGRATION)


def handle_trigger(event, context):
    setup_lambda_logging_middleware(context)
    logger = get_logger(__name__)
    logger.info('handle_trigger', extra={"event": event})
    return trigger_dispatcher.handle(event, context)
