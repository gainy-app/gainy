from gainy.utils import setup_exception_logger_hook

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
from web import *
from trading.actions import *

setup_exception_logger_hook()

ENV = os.environ['ENV']

ALGOLIA_APP_ID = os.getenv("ALGOLIA_APP_ID")
ALGOLIA_TICKERS_INDEX = os.getenv("ALGOLIA_TICKERS_INDEX")
ALGOLIA_COLLECTIONS_INDEX = os.getenv("ALGOLIA_COLLECTIONS_INDEX")
ALGOLIA_SEARCH_API_KEY = os.getenv("ALGOLIA_SEARCH_API_KEY")

GNEWS_API_TOKEN = os.getenv("GNEWS_API_TOKEN")
REDIS_CACHE_HOST = os.getenv("REDIS_CACHE_HOST")
REDIS_CACHE_PORT = os.getenv("REDIS_CACHE_PORT")

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

    # Portfolio
    CreatePlaidLinkToken(),
    LinkPlaidAccount(),
    GetPortfolioHoldings(),
    GetPortfolioTransactions(),
    GetPortfolioChart(),
    GetPortfolioChartPreviousPeriodClose(),
    GetPortfolioPieChart(),
    PlaidWebhook(),

    # Web
    StripeGetCheckoutUrl(),
    StripeWebhook(),

    # Search
    SearchTickers(ALGOLIA_APP_ID, ALGOLIA_SEARCH_API_KEY,
                  ALGOLIA_TICKERS_INDEX),
    SearchCollections(ALGOLIA_APP_ID, ALGOLIA_SEARCH_API_KEY,
                      ALGOLIA_COLLECTIONS_INDEX),
    SearchNews(GNEWS_API_TOKEN, REDIS_CACHE_HOST, REDIS_CACHE_PORT),

    # Managed Portfolio
    KycGetFormConfig(),
    KycGetStatus(),
    KycSendForm(),
    KycAddDocument(),
    TradingLinkBankAccountWithPlaid(),
    TradingGetFundingAccounts(),
    TradingDeleteFundingAccount(),
    TradingDepositFunds(),
    TradingWithdrawFunds(),
    TradingReconfigureCollectionHoldings(),
    TradingGetActualCollectionHoldings(),
    TradingSyncProviderData(),
]

action_dispatcher = HasuraActionDispatcher(ACTIONS,
                                           API_GATEWAY_PROXY_INTEGRATION)


def handle_action(event, context):
    return action_dispatcher.handle(event, context)


TRIGGERS = [
    SetUserCategories(),
    OnUserCreated(ENV),
    SetRecommendations(),
    OnPlaidAccessTokenCreated(),
    OnInvitationCreatedOrUpdated(),
]

trigger_dispatcher = HasuraTriggerDispatcher(TRIGGERS,
                                             API_GATEWAY_PROXY_INTEGRATION)


def handle_trigger(event, context):
    return trigger_dispatcher.handle(event, context)
