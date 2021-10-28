import os
import plaid
from plaid.api import plaid_api
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest

from portfolio.plaid.common import get_plaid_client, handle_error
from common.hasura_function import HasuraAction


class LinkPlaidAccount(HasuraAction):
    def __init__(self):
        super().__init__("link_plaid_account", "profile_id")

        self.client = get_plaid_client()

    def apply(self, db_conn, input_params):
        profile_id = input_params["profile_id"]
        public_token = input_params["public_token"]

        try:
            exchange_request = ItemPublicTokenExchangeRequest(
                public_token=public_token)
            exchange_response = self.client.item_public_token_exchange(
                exchange_request)
        except plaid.ApiException as e:
            handle_error(e)

        with db_conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO app.profile_plaid_access_tokens(profile_id, access_token, item_id) "
                "VALUES (%(profile_id)s, %(access_token)s, %(item_id)s)", {
                    "profile_id": profile_id,
                    "access_token": exchange_response['access_token'],
                    "item_id": exchange_response['item_id']
                })

        return {'result': True}
