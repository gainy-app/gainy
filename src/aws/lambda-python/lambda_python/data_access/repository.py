from psycopg2 import sql
from psycopg2.extras import execute_values

from data_access.models import AbstractBaseModel


class Repository:

    def persist(self, db_conn, entities):
        if isinstance(entities, AbstractBaseModel):
            entities = [entities]

        entities_grouped = self.__group_entities(entities)

        with db_conn.cursor() as cursor:
            for (schema_name,
                 table_name), entities in entities_grouped.items():

                field_names = [
                    field_name for field_name in entities[0].to_dict().keys()
                    if field_name not in entities[0].db_excluded_fields
                ]
                field_names_escaped = self._escape_fields(field_names)

                sql_string = sql.SQL(
                    "INSERT INTO {schema_name}.{table_name} ({field_names}) VALUES %s"
                ).format(schema_name=sql.Identifier(schema_name),
                         table_name=sql.Identifier(table_name),
                         field_names=field_names_escaped)

                key_fields = entities[0].key_fields
                if key_fields:
                    key_field_names_escaped = self._escape_fields(key_fields)

                    sql_string = sql_string + sql.SQL(
                        " ON CONFLICT({key_field_names}) DO UPDATE SET {set_clause}"
                    ).format(
                        key_field_names=key_field_names_escaped,
                        set_clause=sql.SQL(',').join([
                            sql.SQL("{field_name} = excluded.{field_name}").
                                format(field_name=sql.Identifier(field_name))
                            for field_name in field_names
                            if field_name not in key_fields
                        ]))

                db_excluded_fields = entities[0].db_excluded_fields
                if db_excluded_fields:
                    db_excluded_fields_escaped = self._escape_fields(db_excluded_fields)
                    sql_string = sql_string + sql.SQL(
                        " RETURNING {db_excluded_fields}"
                    ).format(db_excluded_fields=db_excluded_fields_escaped)

                entity_dicts = [entity.to_dict() for entity in entities]
                values = [[
                    entity_dict[field_name] for field_name in field_names
                ] for entity_dict in entity_dicts]

                execute_values(cursor, sql_string, values)

                if db_excluded_fields:
                    returned = cursor.fetchall()

                    for entity, returned_row in zip(entities, returned):
                        for db_excluded_field, value in zip(db_excluded_fields, returned_row):
                            entity.__setattr__(db_excluded_field, value)

    @staticmethod
    def _escape_fields(field_names):
        field_names_escaped = sql.SQL(',').join(map(sql.Identifier, field_names))
        return field_names_escaped

    def __group_entities(self, entities):
        entities_grouped = {}

        for entity in entities:
            key = (entity.schema_name, entity.table_name)
            if key in entities_grouped:
                entities_grouped[key].append(entity)
            else:
                entities_grouped[key] = [entity]

        return entities_grouped
