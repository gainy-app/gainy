CREATE TABLE "app"."influencers"
(
    "id"         serial      NOT NULL,
    "profile_id" integer,
    "email"      varchar,
    "name"       varchar     NOT NULL,
    "created_at" timestamptz NOT NULL DEFAULT now(),
    "updated_at" timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE set null ON DELETE set null
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

CREATE TRIGGER "set_app_influencers_updated_at"
    BEFORE UPDATE
    ON "app"."influencers"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();

COMMENT ON TRIGGER "set_app_influencers_updated_at" ON "app"."influencers"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';
