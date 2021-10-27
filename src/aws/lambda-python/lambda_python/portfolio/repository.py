from portfolio.models import BaseModel
from psycopg2.extras import execute_values
from psycopg2 import sql


class PortfolioRepository:
    def persist(self, db_conn, entities):
        if isinstance(entities, BaseModel):
            entities = [entity]

        entities_grouped = self.__group_entities(entities)

        with db_conn.cursor() as cursor:
            for (schema_name,
                 table_name), entities in entities_grouped.items():

                field_names = [
                    field_name for field_name in entities[0].to_dict().keys()
                    if field_name not in entities[0].db_excluded_fields()
                ]
                field_names_escaped = sql.SQL(',').join(
                    map(sql.Identifier, field_names))

                id_field = entities[0].id_field()
                unique_field_names = entities[0].unique_field_names()

                sql_string = sql.SQL(
                    "INSERT INTO {schema_name}.{table_name} ({field_names}) VALUES %s"
                ).format(schema_name=sql.Identifier(schema_name),
                         table_name=sql.Identifier(table_name),
                         field_names=field_names_escaped)

                if len(unique_field_names):
                    unique_field_names_escaped = sql.SQL(',').join(
                        map(sql.Identifier, unique_field_names))
                    print(unique_field_names_escaped)

                    sql_string = sql_string + sql.SQL(
                        " ON CONFLICT({unique_field_names}) DO UPDATE SET {set_clause}"
                    ).format(
                        unique_field_names=unique_field_names_escaped,
                        set_clause=sql.SQL(',').join([
                            sql.SQL("{field_name} = excluded.{field_name}").
                            format(field_name=sql.Identifier(field_name))
                            for field_name in field_names
                            if field_name != id_field
                        ]))

                sql_string = sql_string + sql.SQL(
                    " RETURNING id, created_at, updated_at")

                entity_dicts = [entity.to_dict() for entity in entities]
                print(str(sql_string))

                execute_values(
                    cursor, sql_string,
                    [[entity_dict[field_name] for field_name in field_names]
                     for entity_dict in entity_dicts])

                returned = cursor.fetchall()

                for entity, returned_row in zip(entities, returned):
                    id, created_at, updated_at = returned_row
                    entity.id = id
                    entity.created_at = created_at
                    entity.updated_at = updated_at

    def remove_other_by_profile_id(self, db_conn, entities):
        if isinstance(entities, BaseModel):
            entities = [entity]

        entities_grouped = self.__group_entities(entities)

        with db_conn.cursor() as cursor:
            for (schema_name,
                 table_name), entities in entities_grouped.items():

                profile_id = entities[0].profile_id
                excluded_ids = [entity.id for entity in entities if entity.id]

                sql_string = sql.SQL(
                    "DELETE FROM {schema_name}.{table_name} WHERE profile_id = %(profile_id)s AND id not in %(excluded_ids)s"
                ).format(schema_name=sql.Identifier(schema_name),
                         table_name=sql.Identifier(table_name))

                cursor.execute(sql_string, {
                    'profile_id': profile_id,
                    'excluded_ids': tuple(excluded_ids)
                })

    def __group_entities(self, entities):
        entities_grouped = {}
        for entity in entities:
            key = (entity.schema_name(), entity.table_name())
            if key in entities_grouped:
                entities_grouped[key].append(entity)
            else:
                entities_grouped[key] = [entity]

        return entities_grouped
