CREATE TABLE "app"."drivewealth_autopilot_runs"
(
    "ref_id"                varchar     NOT NULL,
    "account_id"            varchar     NOT NULL,
    "collection_version_id" int         NOT NULL,
    "status"                varchar     NOT NULL,
    "data"                  json,
    "created_at"            timestamptz NOT NULL DEFAULT now(),
    "updated_at"            timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("ref_id"),
    FOREIGN KEY ("collection_version_id") REFERENCES "app"."trading_collection_versions" ("id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("account_id") REFERENCES "app"."drivewealth_accounts" ("ref_id") ON UPDATE cascade ON DELETE cascade
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
CREATE TRIGGER "set_app_drivewealth_autopilot_runs_updated_at"
    BEFORE UPDATE
    ON "app"."drivewealth_autopilot_runs"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_drivewealth_autopilot_runs_updated_at" ON "app"."drivewealth_autopilot_runs"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';

