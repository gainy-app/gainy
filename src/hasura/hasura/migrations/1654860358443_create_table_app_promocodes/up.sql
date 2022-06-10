CREATE TABLE "app"."promocodes"
(
    "id"            serial      NOT NULL,
    "code"          varchar     NOT NULL,
    "influencer_id" integer,
    "description"   text,
    "name"          varchar,
    "created_at"    timestamptz NOT NULL DEFAULT now(),
    "updated_at"    timestamptz NOT NULL DEFAULT now(),
    "config"        jsonb,
    "is_active"     bool,
    PRIMARY KEY ("id"),
    FOREIGN KEY ("influencer_id") REFERENCES "app"."influencers" ("id") ON UPDATE cascade ON DELETE cascade,
    UNIQUE ("code")
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

CREATE TRIGGER "set_app_promocodes_updated_at"
    BEFORE UPDATE
    ON "app"."promocodes"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();

COMMENT ON TRIGGER "set_app_promocodes_updated_at" ON "app"."promocodes"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';
