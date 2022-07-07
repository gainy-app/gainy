from psycopg2 import sql

from gainy.data_access.models import BaseModel
from gainy.data_access.repository import Repository


class PortfolioRepository(Repository):

    def remove_other_by_access_token(self, db_conn, entities):
        if isinstance(entities, BaseModel):
            entities = [entities]

        entities_grouped = self._group_entities(entities)

        with db_conn.cursor() as cursor:
            for (schema_name,
                 table_name), entities in entities_grouped.items():

                plaid_access_token_ids = set(
                    [entity.plaid_access_token_id for entity in entities])
                excluded_ids = set(
                    [entity.id for entity in entities if entity.id])

                sql_string = sql.SQL(
                    "DELETE FROM {schema_name}.{table_name} WHERE plaid_access_token_id IN %(plaid_access_token_ids)s AND id NOT IN %(excluded_ids)s"
                ).format(schema_name=sql.Identifier(schema_name),
                         table_name=sql.Identifier(table_name))
                params = {
                    'plaid_access_token_ids': tuple(plaid_access_token_ids),
                }

                if excluded_ids:
                    sql_string += sql.SQL(" AND id NOT IN %(excluded_ids)s")
                    params['excluded_ids'] = tuple(excluded_ids)

                cursor.execute(sql_string, params)
