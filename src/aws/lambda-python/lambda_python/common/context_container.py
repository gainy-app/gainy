import os
from functools import cached_property

from _stripe.api import StripeApi
from gainy.utils import env, ENV_PRODUCTION
from portfolio.plaid.service import PlaidService
from portfolio.service import PortfolioService
from portfolio.service.chart import PortfolioChartService
from portfolio.repository import PortfolioRepository
from queue_processing.dispatcher import QueueMessageDispatcher
from services.cache import Cache, RedisCache, LocalCache
from services.notification import NotificationService
from services.sendgrid import SendGridService
from services.sqs import SqsAdapter
from services.twilio import TwilioClient
from trading.drivewealth.queue_message_handler import DriveWealthQueueMessageHandler
from trading.service import TradingService
from trading.repository import TradingRepository
from trading.drivewealth.api import DriveWealthApi
from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.repository import DriveWealthRepository

from gainy.context_container import ContextContainer as GainyContextContainer
from verification.client.email import EmailVerificationClient
from verification.client.sms import SmsVerificationClient
from verification.service import VerificationService

REDIS_CACHE_HOST = os.getenv("REDIS_CACHE_HOST")
REDIS_CACHE_PORT = os.getenv("REDIS_CACHE_PORT")


class ContextContainer(GainyContextContainer):
    request = None
    headers: dict = None

    @cached_property
    def stripe_api(self) -> StripeApi:
        return StripeApi()

    @cached_property
    def twilio_client(self) -> TwilioClient:
        return TwilioClient()

    @cached_property
    def sendgrid_service(self) -> SendGridService:
        return SendGridService()

    @cached_property
    def notification_service(self) -> NotificationService:
        return NotificationService(self.sendgrid_service)

    @cached_property
    def cache(self) -> Cache:
        if REDIS_CACHE_HOST and REDIS_CACHE_PORT:
            return RedisCache(REDIS_CACHE_HOST, REDIS_CACHE_PORT)

        return LocalCache()

    # verification
    @cached_property
    def sms_verification_client(self) -> SmsVerificationClient:
        return SmsVerificationClient(self.twilio_client)

    @cached_property
    def email_verification_client(self) -> EmailVerificationClient:
        return EmailVerificationClient(self.twilio_client)

    @cached_property
    def verification_service(self) -> VerificationService:
        return VerificationService(self.get_repository(), [
            self.sms_verification_client,
            self.email_verification_client,
        ],
                                   env() != ENV_PRODUCTION)

    ## portfolio
    @cached_property
    def portfolio_repository(self) -> PortfolioRepository:
        return PortfolioRepository(self.db_conn)

    @cached_property
    def plaid_service(self) -> PlaidService:
        return PlaidService(self.db_conn)

    @cached_property
    def portfolio_service(self) -> PortfolioService:
        return PortfolioService(self.db_conn, self.portfolio_repository,
                                self.plaid_service)

    @cached_property
    def portfolio_chart_service(self) -> PortfolioChartService:
        return PortfolioChartService(self.db_conn)

    ## drivewealth
    @cached_property
    def drivewealth_repository(self):
        return DriveWealthRepository(self.db_conn)

    @cached_property
    def drivewealth_api(self):
        return DriveWealthApi(self.drivewealth_repository)

    @cached_property
    def drivewealth_provider(self):
        return DriveWealthProvider(self.drivewealth_repository,
                                   self.drivewealth_api, self.plaid_service,
                                   self.notification_service)

    ## trading
    @cached_property
    def trading_service(self) -> TradingService:
        return TradingService(self.trading_repository,
                              self.drivewealth_provider, self.plaid_service)

    @cached_property
    def trading_repository(self) -> TradingRepository:
        return TradingRepository(self.db_conn)

    # queues
    @cached_property
    def sqs_adapter(self) -> SqsAdapter:
        return SqsAdapter(self.get_repository())

    @cached_property
    def drivewealth_queue_message_handler(
            self) -> DriveWealthQueueMessageHandler:
        return DriveWealthQueueMessageHandler(self.drivewealth_repository,
                                              self.drivewealth_provider)

    @cached_property
    def queue_message_dispatcher(self) -> QueueMessageDispatcher:
        return QueueMessageDispatcher([self.drivewealth_queue_message_handler])
