from gainy.plaid.common import PURPOSE_TRADING, DEFAULT_ENV, get_purpose
from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.utils import get_logger

logger = get_logger(__name__)


class LinkPlaidAccount(HasuraAction):

    def __init__(self):
        super().__init__("link_plaid_account", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        trading_service = context_container.trading_service
        plaid_service = context_container.plaid_service
        db_conn = context_container.db_conn
        profile_id = input_params["profile_id"]
        public_token = input_params["public_token"]
        env = input_params.get("env", DEFAULT_ENV)  # default for legacy app
        access_token_id = input_params.get("access_token_id")
        purpose = get_purpose(input_params)

        response = plaid_service.exchange_public_token(public_token, env)
        access_token = response['access_token']

        parameters = {
            "profile_id": profile_id,
            "access_token": access_token,
            "item_id": response['item_id'],
            "purpose": purpose,
        }
        if access_token_id is None:
            query = """INSERT INTO app.profile_plaid_access_tokens(profile_id, access_token, item_id, purpose)
                    VALUES (%(profile_id)s, %(access_token)s, %(item_id)s, %(purpose)s) RETURNING id"""
        else:
            query = """update app.profile_plaid_access_tokens set access_token = %(access_token)s, item_id = %(item_id)s, needs_reauth_since = null
                    where profile_id = %(profile_id)s and id = %(access_token_id)s RETURNING id"""
            parameters["access_token_id"] = access_token_id

        with db_conn.cursor() as cursor:
            cursor.execute(query, parameters)
            returned = cursor.fetchall()
            id = returned[0][0]

        accounts = []
        if purpose == PURPOSE_TRADING:
            accounts = plaid_service.get_item_accounts(access_token)
            accounts = trading_service.filter_existing_funding_accounts(
                accounts)
            accounts = [i.to_dict() for i in accounts]

        return {
            'result': True,
            'plaid_access_token_id': id,
            "accounts": accounts
        }
