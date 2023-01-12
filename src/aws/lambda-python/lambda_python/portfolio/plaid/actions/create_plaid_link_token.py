import plaid

from portfolio.plaid import PlaidClient

from gainy.plaid.common import handle_error, DEFAULT_ENV, get_purpose, get_purpose_products
from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.utils import get_logger

logger = get_logger(__name__)


class CreatePlaidLinkToken(HasuraAction):

    def __init__(self):
        super().__init__("create_plaid_link_token", "profile_id")
        self.client = PlaidClient()

    def apply(self, input_params, context_container: ContextContainer):
        plaid_service = context_container.plaid_service
        profile_id = input_params["profile_id"]
        redirect_uri = input_params["redirect_uri"]
        env = input_params.get("env", DEFAULT_ENV)  # default for legacy app
        access_token_id = input_params.get("access_token_id")

        purpose = get_purpose(input_params)
        products = get_purpose_products(purpose)

        access_token = None
        if access_token_id is not None:
            access_token = plaid_service.get_access_token(id=access_token_id)
            if access_token and access_token['profile_id'] != profile_id:
                access_token = None

        try:
            response = self.client.create_link_token(profile_id, redirect_uri,
                                                     products, env,
                                                     access_token)

            return {'link_token': response['link_token']}
        except plaid.ApiException as e:
            handle_error(e)
