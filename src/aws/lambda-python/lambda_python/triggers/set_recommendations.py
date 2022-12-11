from common.context_container import ContextContainer
from common.hasura_function import HasuraTrigger
from gainy.utils import get_logger

logger = get_logger(__name__)


# Deprecated
def skip_interest_update(db_conn, profile_id, interest_id):
    if interest_id is None:
        return False

    with db_conn.cursor() as cursor:
        cursor.execute(
            "select skip_trigger from app.profile_interests where profile_id = %(profile_id)s and interest_id = %(interest_id)s",
            {
                "profile_id": profile_id,
                "interest_id": interest_id
            })
        row = cursor.fetchone()

    return row and row[0]


def skip_category_update(db_conn, profile_id, category_id):
    if category_id is None:
        return False

    with db_conn.cursor() as cursor:
        cursor.execute(
            "select skip_trigger from app.profile_categories where profile_id = %(profile_id)s and category_id = %(category_id)s",
            {
                "profile_id": profile_id,
                "category_id": category_id
            })
        row = cursor.fetchone()

    return row and row[0]


class SetRecommendations(HasuraTrigger):

    def __init__(self):
        super().__init__([
            "recommendations__profile_categories",
            "recommendations__profile_interests"
        ])

    def apply(self, op, data, context_container: ContextContainer):
        db_conn = context_container.db_conn
        profile_id = self.get_profile_id(data)
        payload = self._extract_payload(data)
        category_id = payload.get("category_id")
        interest_id = payload.get("interest_id")

        logging_extra = {
            'function': 'SetRecommendations',
            'profile_id': profile_id,
            'category_id': category_id,
            'interest_id': interest_id,
        }

        skip_trigger = skip_category_update(
            db_conn, profile_id, category_id) or skip_interest_update(
                db_conn, profile_id, interest_id)
        if skip_trigger:
            return

        logger.info('SetRecommendations', extra=logging_extra)
        context_container.recommendation_service.compute_match_score(
            profile_id, max_tries=5)

    def get_profile_id(self, data):
        payload = self._extract_payload(data)
        return payload["profile_id"]

    def get_allowed_profile_ids(self, op, data):
        return self.get_profile_id(data)
