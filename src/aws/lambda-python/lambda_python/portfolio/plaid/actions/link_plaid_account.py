from gainy.plaid.common import PURPOSE_TRADING, DEFAULT_ENV, get_purpose
from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.plaid.models import PlaidAccessToken
from gainy.utils import get_logger
from portfolio.models import Account

logger = get_logger(__name__)


class LinkPlaidAccount(HasuraAction):

    def __init__(self):
        super().__init__("link_plaid_account", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        trading_service = context_container.trading_service
        plaid_service = context_container.plaid_service
        portfolio_service = context_container.portfolio_service
        portfolio_repository = context_container.portfolio_repository
        profile_id = input_params["profile_id"]
        public_token = input_params["public_token"]
        env = input_params.get("env", DEFAULT_ENV)  # default for legacy app
        access_token_id = input_params.get("access_token_id")
        purpose = get_purpose(input_params)

        response = plaid_service.exchange_public_token(public_token, env)
        access_token: str = response['access_token']

        if access_token_id is None:
            entity = PlaidAccessToken()
            entity.profile_id = profile_id
        else:
            entity = portfolio_repository.find_one(
                PlaidAccessToken,
                {"id": access_token_id}) or PlaidAccessToken()
        entity.access_token = access_token
        entity.item_id = response['item_id']
        entity.purpose = purpose
        portfolio_repository.persist(entity)

        if access_token_id:
            context_container.plaid_service.set_access_token_reauth(
                entity, False)
            entity = portfolio_repository.refresh(entity)

        access_token_id = entity.id

        institution = portfolio_service.sync_institution(
            plaid_service.get_access_token(id=access_token_id))

        accounts = []
        if purpose == PURPOSE_TRADING:
            accounts = plaid_service.get_item_accounts(entity)

            account_entities = []
            for account in accounts:
                account_entity = Account()
                account_entity.ref_id = account.account_id
                account_entity.balance_available = account.balance_available
                account_entity.balance_current = account.balance_current
                account_entity.balance_iso_currency_code = account.iso_currency_code
                account_entity.balance_limit = account.balance_limit
                account_entity.mask = account.mask
                account_entity.name = account.name
                account_entity.official_name = account.official_name
                account_entity.subtype = account.subtype
                account_entity.type = account.type
                account_entity.profile_id = profile_id
                account_entity.plaid_access_token_id = access_token_id
                account_entities.append(account_entity)
            portfolio_repository.persist(
                portfolio_service.unique_entities(account_entities))

            accounts = trading_service.filter_existing_funding_accounts(
                accounts)
            accounts = [i.to_dict() for i in accounts]

        return {
            'result': True,
            'plaid_access_token_id': access_token_id,
            'institution_name': institution.name,
            "accounts": accounts
        }
