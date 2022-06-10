import json
import logging
from common.hasura_function import HasuraAction
from psycopg2.extras import RealDictCursor

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class GetPromocode(HasuraAction):

    def __init__(self):
        super().__init__("get_promocode")

    def apply(self, db_conn, input_params, headers):
        code = input_params["code"]

        with db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "select id, description, name, config from app.promocodes where code = %(code)s and is_active = true",
                {"code": code})

            row = cursor.fetchone()

        if row is None:
            return None

        return {
            "id": row['id'],
            "description": row['description'],
            "name": row['name'],
            "config": json.dumps(row['config']),
        }
