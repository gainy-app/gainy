from psycopg2.extras import execute_values
from common.context_container import ContextContainer
from common.hasura_function import HasuraTrigger
from gainy.utils import get_logger

logger = get_logger(__name__)


class SetUserCategories(HasuraTrigger):

    def __init__(self):
        super().__init__("set_user_categories")

    def get_allowed_profile_ids(self, op, data):
        return data["new"]['profile_id']

    def apply(self, op, data, context_container: ContextContainer):
        db_conn = context_container.db_conn
        payload = self._extract_payload(data)
        service = context_container.recommendation_service

        profile_id = payload["profile_id"]
        risk_score = service.calculate_risk_score(payload)

        with db_conn.cursor() as cursor:
            cursor.execute(
                "select id from categories where risk_score = %(risk_score)s",
                {'risk_score': risk_score})

            rows = cursor.fetchall()
            categories = [row[0] for row in rows]

        logging_extra = {
            'profile_id': profile_id,
            'payload': payload,
            'risk_score': risk_score,
            'categories': categories,
        }
        logger.info('set_user_categories', extra=logging_extra)

        with db_conn.cursor() as cursor:
            cursor.execute(
                "delete from app.profile_categories where profile_id = %(profile_id)s",
                {'profile_id': profile_id})

            execute_values(
                cursor,
                "INSERT INTO app.profile_categories (profile_id, category_id, skip_trigger) VALUES %s",
                [(profile_id, category_id, True)
                 for category_id in categories])

        service.compute_match_score(profile_id)