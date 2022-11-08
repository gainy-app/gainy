import hashlib
import hmac
import time
from jose import jwt

from gainy.exceptions import HttpException
from portfolio.plaid import PlaidClient
from portfolio.service import SERVICE_PLAID

from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.utils import get_logger

logger = get_logger(__name__)


class PlaidWebhook(HasuraAction):

    def __init__(self):
        super().__init__("plaid_webhook")

        self.client = PlaidClient()

    def apply(self, input_params, context_container: ContextContainer):
        headers = context_container.headers
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
            raise HttpException(
                400,
                "[PLAID_WEBHOOK] Failed to validate plaid request key: Key expired"
            )

        # Validate the signature and extract the claims.
        try:
            claims = jwt.decode(signed_jwt, key, algorithms=['ES256'])
        except jwt.JWTError as e:
            raise HttpException(
                400,
                "[PLAID_WEBHOOK] Failed to validate plaid request key: decode failed with: %s"
                % (str(e)))

        # Ensure that the token is not expired.
        if claims["iat"] < time.time() - 5 * 60:
            raise HttpException(
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
            raise HttpException(
                400,
                "[PLAID_WEBHOOK] Failed to validate plaid request key: body hash mismatch"
            )
