CREATE TABLE "app"."drivewealth_redemptions"
(
    "ref_id"                 varchar     NOT NULL,
    "trading_account_ref_id" varchar     NOT NULL,
    "bank_account_ref_id"    varchar,
    "money_flow_id"          int,
    "status"                 varchar     NOT NULL,
    "data"                   json,
    "created_at"             timestamptz NOT NULL DEFAULT now(),
    "updated_at"             timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("ref_id"),
    FOREIGN KEY ("trading_account_ref_id") REFERENCES "app"."drivewealth_accounts" ("ref_id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("bank_account_ref_id") REFERENCES "app"."drivewealth_bank_accounts" ("ref_id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("money_flow_id") REFERENCES "app"."trading_money_flow" ("id") ON UPDATE restrict ON DELETE restrict
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
CREATE TRIGGER "set_app_drivewealth_redemptions_updated_at"
    BEFORE UPDATE
    ON "app"."drivewealth_redemptions"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_drivewealth_redemptions_updated_at" ON "app"."drivewealth_redemptions"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';

