import os
import hashlib
import hmac
import time
from jose import jwt

import plaid
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.country_code import CountryCode
from plaid.model.products import Products
from portfolio.plaid import PlaidClient, PlaidService
from portfolio.service import PortfolioService, SERVICE_PLAID

from portfolio.plaid.common import get_plaid_client, handle_error
from common.hasura_function import HasuraAction
from common.hasura_exception import HasuraActionException

PLAID_WEBHOOK_URL = os.getenv("PLAID_WEBHOOK_URL")


class CreatePlaidLinkToken(HasuraAction):
    def __init__(self):
        super().__init__("create_plaid_link_token", "profile_id")

        self.client = get_plaid_client()

    def apply(self, db_conn, input_params, headers):
        profile_id = input_params["profile_id"]
        redirect_uri = input_params["redirect_uri"]

        try:
            #TODO when we have verified phone number, we can implement https://plaid.com/docs/link/returning-user/#enabling-the-returning-user-experience
            request = LinkTokenCreateRequest(
                products=[Products('investments')],
                client_name="Gainy",
                country_codes=[CountryCode('US')],
                language='en',
                redirect_uri=redirect_uri,
                webhook=PLAID_WEBHOOK_URL,
                user=LinkTokenCreateRequestUser(
                    client_user_id=str(profile_id), ))
            # create link token
            response = self.client.link_token_create(request)
            link_token = response['link_token']

            return {'link_token': response['link_token']}
        except plaid.ApiException as e:
            handle_error(e)


class LinkPlaidAccount(HasuraAction):
    def __init__(self):
        super().__init__("link_plaid_account", "profile_id")

        self.client = get_plaid_client()

    def apply(self, db_conn, input_params, headers):
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


class PlaidWebhook(HasuraAction):
    def __init__(self):
        super().__init__("plaid_webhook")

        self.portfolio_service = PortfolioService()
        self.client = PlaidClient()

    def apply(self, db_conn, input_params, headers):
        print(input_params, headers)

        try:
            self.verify(input_params, headers)
            item_id = input_params['item_id']
            with db_conn.cursor() as cursor:
                cursor.execute(
                    "SELECT id, profile_id, access_token FROM app.profile_plaid_access_tokens WHERE item_id = %(item_id)s",
                    {"item_id": item_id})

                access_tokens = [
                    dict(
                        zip(['id', 'profile_id', 'access_token', 'service'],
                            row + (SERVICE_PLAID, )))
                    for row in cursor.fetchall()
                ]

            count = 0
            webhook_type = input_params['webhook_type']
            for access_token in access_tokens:
                if webhook_type == 'HOLDINGS':
                    count += self.portfolio_service.sync_token_holdings(
                        db_conn, access_token)
                elif webhook_type == 'INVESTMENTS_TRANSACTIONS':
                    count += self.portfolio_service.sync_token_transactions(
                        db_conn, access_token)

            return {'count': count}
        except Exception as e:
            print("[PLAID_WEBHOOK] %s" % (e))
            raise e

    def verify(self, body, headers):
        signed_jwt = headers.get('plaid-verification')
        current_key_id = jwt.get_unverified_header(signed_jwt)['kid']

        response = self.client.webhook_verification_key_get(
            current_key_id).to_dict()
        print('[PLAID_WEBHOOK] response', response)

        key = response['key']
        if key['expired_at'] is not None:
            raise HasuraActionException(
                400,
                "[PLAID_WEBHOOK] Failed to validate plaid request key: Key expired"
            )

        # Validate the signature and extract the claims.
        try:
            claims = jwt.decode(signed_jwt, key, algorithms=['ES256'])
            print('claims', claims)
        except jwt.JWTError as e:
            raise HasuraActionException(
                400,
                "[PLAID_WEBHOOK] Failed to validate plaid request key: decode failed with: %s"
                % (str(e)))

        # Ensure that the token is not expired.
        if claims["iat"] < time.time() - 5 * 60:
            raise HasuraActionException(
                400,
                "[PLAID_WEBHOOK] Failed to validate plaid request key: claim expired"
            )

        # Compute the has of the body.
        m = hashlib.sha256()
        m.update(body.encode())
        body_hash = m.hexdigest()
        print('[PLAID_WEBHOOK] body_hash', body_hash)

        # Ensure that the hash of the body matches the claim.
        # Use constant time comparison to prevent timing attacks.
        if not hmac.compare_digest(body_hash, claims['request_body_sha256']):
            raise HasuraActionException(
                400,
                "[PLAID_WEBHOOK] Failed to validate plaid request key: body hash mismatch"
            )
