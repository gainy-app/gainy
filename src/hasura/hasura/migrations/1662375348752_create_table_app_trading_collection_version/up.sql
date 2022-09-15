CREATE TABLE "app"."trading_collection_versions"
(
    "id"                  serial      NOT NULL,
    "profile_id"          int         not null,
    "collection_id"       int         not null,
    "status"              varchar,
    "target_amount_delta" numeric     not null,
    "weights"             json,
    "created_at"          timestamptz NOT NULL DEFAULT now(),
    "updated_at"          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade
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
CREATE TRIGGER "set_app_trading_collection_versions_updated_at"
    BEFORE UPDATE
    ON "app"."trading_collection_versions"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_trading_collection_versions_updated_at" ON "app"."trading_collection_versions"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';
