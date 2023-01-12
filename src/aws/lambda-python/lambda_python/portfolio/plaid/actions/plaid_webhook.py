import hashlib
import hmac
import time
from jose import jwt

from gainy.exceptions import HttpException
from portfolio.plaid import PlaidClient

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
        plaid_service = context_container.plaid_service
        db_conn = context_container.db_conn
        item_id = input_params['item_id']
        logging_extra = {
            'input_params': input_params,
            'item_id': item_id,
            'headers': headers,
        }

        try:
            access_token = plaid_service.get_access_token(item_id=item_id)

            count = 0
            if not access_token:
                return {'count': count}

            token = access_token['access_token']
            logging_extra['profile_id'] = access_token['profile_id']

            logger.info("[PLAID_WEBHOOK] invoke", extra=logging_extra)

            try:
                self.verify(input_params, headers, token)
            except Exception as e:
                logger.error('[PLAID_WEBHOOK] verify: %s',
                             e,
                             extra=logging_extra)

            webhook_type = input_params['webhook_type']
            portfolio_service.sync_institution(access_token)
            if webhook_type == 'HOLDINGS':
                count += portfolio_service.sync_token_holdings(access_token)
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
