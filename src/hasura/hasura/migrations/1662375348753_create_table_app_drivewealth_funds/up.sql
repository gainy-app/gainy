CREATE TABLE "app"."drivewealth_funds"
(
    "ref_id"                        varchar     NOT NULL,
    "drivewealth_user_id"           varchar     NOT NULL,
    "collection_id"                 int         NOT NULL,
    "trading_collection_version_id" int         NOT NULL,
    "holdings"                      json,
    "weights"                       json,
    "data"                          json,
    "created_at"                    timestamptz NOT NULL DEFAULT now(),
    "updated_at"                    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("ref_id"),
    constraint "drivewealth_funds_drivewealth_user_id_collection_id" unique ("drivewealth_user_id", "collection_id"),
    FOREIGN KEY ("drivewealth_user_id") REFERENCES "app"."drivewealth_users" ("ref_id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("trading_collection_version_id") REFERENCES "app"."trading_collection_versions" ("id") ON UPDATE cascade ON DELETE cascade
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
CREATE TRIGGER "set_app_drivewealth_funds_updated_at"
    BEFORE UPDATE
    ON "app"."drivewealth_funds"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_drivewealth_funds_updated_at" ON "app"."drivewealth_funds"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';

