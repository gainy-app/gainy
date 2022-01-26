import os
import hashlib
import hmac
import time
from jose import jwt

import plaid
from portfolio.plaid import PlaidClient, PlaidService
from portfolio.service import PortfolioService, SERVICE_PLAID

from portfolio.plaid.common import get_plaid_client, handle_error
from common.hasura_function import HasuraAction
from common.hasura_exception import HasuraActionException
from service.logging import get_logger

logger = get_logger(__name__)


class CreatePlaidLinkToken(HasuraAction):

    def __init__(self):
        super().__init__("create_plaid_link_token", "profile_id")
        self.client = PlaidClient()

    def apply(self, db_conn, input_params, headers):
        profile_id = input_params["profile_id"]
        redirect_uri = input_params["redirect_uri"]

        try:
            response = self.client.create_link_token(profile_id, redirect_uri)
            link_token = response['link_token']

            return {'link_token': response['link_token']}
        except plaid.ApiException as e:
            handle_error(e)


class LinkPlaidAccount(HasuraAction):

    def __init__(self):
        super().__init__("link_plaid_account", "profile_id")
        self.client = PlaidClient()

    def apply(self, db_conn, input_params, headers):
        profile_id = input_params["profile_id"]
        public_token = input_params["public_token"]

        try:
            response = self.client.exchange_link_token(public_token)
        except plaid.ApiException as e:
            handle_error(e)

        with db_conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO app.profile_plaid_access_tokens(profile_id, access_token, item_id) "
                "VALUES (%(profile_id)s, %(access_token)s, %(item_id)s) RETURNING id",
                {
                    "profile_id": profile_id,
                    "access_token": response['access_token'],
                    "item_id": response['item_id'],
                })
            returned = cursor.fetchall()
            id = returned[0][0]

        return {'result': True, 'plaid_access_token_id': id}


class PlaidWebhook(HasuraAction):

    def __init__(self):
        super().__init__("plaid_webhook")

        self.portfolio_service = PortfolioService()
        self.client = PlaidClient()

    def apply(self, db_conn, input_params, headers):
        print("[PLAID_WEBHOOK] %s" % (input_params))

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
                self.portfolio_service.sync_institution(db_conn, access_token)
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

        return  # check skipped as we currently don't have raw body to compute hash from

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
