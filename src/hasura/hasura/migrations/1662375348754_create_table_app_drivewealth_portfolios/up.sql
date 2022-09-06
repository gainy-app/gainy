CREATE TABLE "app"."drivewealth_portfolios"
(
    "ref_id"                 varchar     NOT NULL,
    "drivewealth_user_id"    varchar     NOT NULL,
    "drivewealth_account_id" varchar     NOT NULL,
    "holdings"               json,
    "data"                   json,
    "cash_target_weight"     numeric,
    "created_at"             timestamptz NOT NULL DEFAULT now(),
    "updated_at"             timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("ref_id"),
    FOREIGN KEY ("drivewealth_user_id") REFERENCES "app"."drivewealth_users" ("ref_id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("drivewealth_account_id") REFERENCES "app"."drivewealth_accounts" ("ref_id") ON UPDATE cascade ON DELETE cascade
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
CREATE TRIGGER "set_app_drivewealth_portfolios_updated_at"
    BEFORE UPDATE
    ON "app"."drivewealth_portfolios"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_drivewealth_portfolios_updated_at" ON "app"."drivewealth_portfolios"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';

