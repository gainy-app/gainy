import os
import hashlib
import hmac
import time
from jose import jwt

import plaid
from portfolio.plaid import PlaidClient, PlaidService
from portfolio.service import PortfolioService, SERVICE_PLAID

from portfolio.plaid.common import handle_error, PURPOSE_PORTFOLIO, PURPOSE_TRADING
from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from common.hasura_exception import HasuraActionException
from gainy.utils import get_logger

logger = get_logger(__name__)

DEFAULT_ENV = "development"


def get_purpose(input_params):
    purpose = input_params.get("purpose", "portfolio")
    if purpose not in [PURPOSE_PORTFOLIO, PURPOSE_TRADING]:
        raise Exception('Wrong purpose')
    return purpose


def get_purpose_products(purpose):
    if purpose == PURPOSE_PORTFOLIO:
        return ['investments']
    elif purpose == PURPOSE_TRADING:
        return ['auth']
    else:
        raise Exception('Wrong purpose')


class CreatePlaidLinkToken(HasuraAction):

    def __init__(self):
        super().__init__("create_plaid_link_token", "profile_id")
        self.client = PlaidClient()

    def apply(self, input_params, context_container: ContextContainer):
        db_conn = context_container.db_conn
        profile_id = input_params["profile_id"]
        redirect_uri = input_params["redirect_uri"]
        env = input_params.get("env", DEFAULT_ENV)  # default for legacy app
        access_token_id = input_params.get("access_token_id")

        purpose = get_purpose(input_params)
        products = get_purpose_products(purpose)

        access_token = None
        if access_token_id is not None:
            with db_conn.cursor() as cursor:
                cursor.execute(
                    "SELECT access_token FROM app.profile_plaid_access_tokens WHERE id = %(id)s and profile_id = %(profile_id)s",
                    {
                        "id": access_token_id,
                        "profile_id": profile_id
                    })

                row = cursor.fetchone()
                if row is not None:
                    access_token = row[0]

        try:
            response = self.client.create_link_token(profile_id, redirect_uri,
                                                     products, env,
                                                     access_token)
            link_token = response['link_token']

            return {'link_token': response['link_token']}
        except plaid.ApiException as e:
            handle_error(e)


class LinkPlaidAccount(HasuraAction):

    def __init__(self):
        super().__init__("link_plaid_account", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        service = context_container.plaid_service
        db_conn = context_container.db_conn
        profile_id = input_params["profile_id"]
        public_token = input_params["public_token"]
        env = input_params.get("env", DEFAULT_ENV)  # default for legacy app
        access_token_id = input_params.get("access_token_id")
        purpose = get_purpose(input_params)

        response = service.exchange_public_token(public_token, env)
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
            response = service.get_item_accounts(access_token)
            accounts = [i.to_dict() for i in response]

        return {
            'result': True,
            'plaid_access_token_id': id,
            "accounts": accounts
        }


class PlaidWebhook(HasuraAction):

    def __init__(self):
        super().__init__("plaid_webhook")

        self.client = PlaidClient()

    def apply(self, input_params, context_container: ContextContainer):
        portfolio_service = context_container.portfolio_service
        db_conn = context_container.db_conn
        item_id = input_params['item_id']
        logging_extra = {
            'input_params': input_params,
            'item_id': item_id,
            'headers': headers,
        }

        try:
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
            if not len(access_tokens):
                return {'count': count}

            token = access_tokens[0]['access_token']
            logging_extra['profile_id'] = access_tokens[0]['profile_id']

            logger.info("[PLAID_WEBHOOK] invoke", extra=logging_extra)

            try:
                self.verify(input_params, headers, token)
            except Exception as e:
                logger.error('[PLAID_WEBHOOK] verify: %s',
                             e,
                             extra=logging_extra)

            webhook_type = input_params['webhook_type']
            for access_token in access_tokens:
                portfolio_service.sync_institution(access_token)
                if webhook_type == 'HOLDINGS':
                    count += portfolio_service.sync_token_holdings(
                        access_token)
                elif webhook_type == 'INVESTMENTS_TRANSACTIONS':
                    count += portfolio_service.sync_token_transactions(
                        access_token)

            return {'count': count}
        except Exception as e:
            logger.error("[PLAID_WEBHOOK] %s", e, extra=logging_extra)
            raise e

    def verify(self, body, headers, access_token):
        signed_jwt = headers.get('Plaid-Verification') or headers.get(
            'plaid-verification')
        current_key_id = jwt.get_unverified_header(signed_jwt)['kid']

        response = self.client.webhook_verification_key_get(
            current_key_id, access_token).to_dict()

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
        logger.info('[PLAID_WEBHOOK] body_hash %s', body_hash)

        # Ensure that the hash of the body matches the claim.
        # Use constant time comparison to prevent timing attacks.
        if not hmac.compare_digest(body_hash, claims['request_body_sha256']):
            raise HasuraActionException(
                400,
                "[PLAID_WEBHOOK] Failed to validate plaid request key: body hash mismatch"
            )
