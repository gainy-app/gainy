CREATE OR REPLACE PROCEDURE clone_schema(source_schema text, dest_schema text) AS
$$
DECLARE
    query text;
    name  text;
BEGIN
    EXECUTE 'CREATE SCHEMA if not exists ' || dest_schema;

    -- copy tables
    FOR name IN
        SELECT table_name::text
        FROM information_schema.TABLES
        WHERE table_schema = source_schema
          and table_type = 'BASE TABLE'
        LOOP
            EXECUTE 'CREATE TABLE if not exists ' || dest_schema || '.' || name || ' as table ' ||
                    source_schema || '.' || name;
            COMMIT;
        END LOOP;

    -- copy views
    FOR query IN
        SELECT 'create or replace view "' || dest_schema || '"."' || viewname || '" as ' ||
               replace(definition, 'ON ' || source_schema || '.', 'ON ' || dest_schema || '.')
        FROM pg_views
        WHERE schemaname = source_schema
        LOOP
            EXECUTE query;
            COMMIT;
        END LOOP;

    -- copy indexes
    FOR query IN
        select replace(pg_get_indexdef(idx.oid), 'ON ' || source_schema || '.', 'ON ' || dest_schema || '.')
        from pg_index ind
                 join pg_class idx on idx.oid = ind.indexrelid
                 join pg_class tbl on tbl.oid = ind.indrelid
                 left join pg_namespace ns on ns.oid = tbl.relnamespace
        where ns.nspname = source_schema
          and not indisprimary
          and not indisunique
        LOOP
            BEGIN
                EXECUTE query;
            EXCEPTION
                WHEN duplicate_table THEN
                WHEN duplicate_object THEN
            END;
            COMMIT;
        END LOOP;

    -- copy constraints
    FOR query, name IN
        select 'alter table "' || dest_schema || '"."' || tbl.relname || '" add constraint ' || conname || ' ' ||
               replace(pg_get_constraintdef(c.oid), 'ON ' || source_schema || '.', 'ON ' || dest_schema || '.'),
               conname
        from pg_constraint c
                 join pg_class tbl on tbl.oid = c.conrelid
                 left join pg_namespace ns on ns.oid = tbl.relnamespace
        where ns.nspname = source_schema
        LOOP
            IF NOT EXISTS(
                    SELECT constraint_schema, constraint_name
                    FROM information_schema.table_constraints
                    WHERE constraint_schema = dest_schema
                      AND constraint_name = name
                )
            THEN
                BEGIN
                    EXECUTE query;
                EXCEPTION
                    WHEN duplicate_table THEN
                    WHEN duplicate_object THEN
                END;
                COMMIT;
            END IF;
        END LOOP;
END ;
$$ LANGUAGE plpgsql;
