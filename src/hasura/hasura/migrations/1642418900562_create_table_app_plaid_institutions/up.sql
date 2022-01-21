CREATE TABLE "app"."plaid_institutions"
(
    "id"            serial      NOT NULL,
    "name"          varchar,
    "ref_id"        varchar     NOT NULL,
    "created_at"    timestamptz null default now(),
    "updated_at"    timestamptz null default now(),
    PRIMARY KEY ("id"),
    UNIQUE ("ref_id")
);

CREATE OR REPLACE FUNCTION "app"."set_current_timestamp_updated_at"()
    RETURNS TRIGGER AS
$$
DECLARE
    _new record;
BEGIN
    _new := NEW;
    _new."updated_at" = NOW();
    RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_app_plaid_institutions_updated_at"
    BEFORE UPDATE
    ON "app"."plaid_institutions"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_plaid_institutions_updated_at" ON "app"."plaid_institutions"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';


alter table "app"."profile_plaid_access_tokens"
    add column "institution_id" integer null;

alter table "app"."profile_plaid_access_tokens"
    add constraint "profile_plaid_access_tokens_institution_id_fkey"
        foreign key ("institution_id")
            references "app"."plaid_institutions"
                ("id") on update set null on delete set null;
