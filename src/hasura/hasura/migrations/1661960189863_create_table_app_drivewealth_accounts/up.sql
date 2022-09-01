CREATE TABLE "app"."drivewealth_accounts"
(

    "ref_id"                        varchar     NOT NULL,
    "drivewealth_user_id"           varchar     NOT NULL,
    "trading_account_id"            integer,
    "status"                        varchar,
    "ref_no"                        varchar,
    "nickname"                      varchar,
    "cash_available_for_trade"      integer,
    "cash_available_for_withdrawal" integer,
    "cash_balance"                  integer,
    "data"                          json        NOT NULL,
    "created_at"                    timestamptz NOT NULL DEFAULT now(),
    "updated_at"                    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("ref_id"),
    FOREIGN KEY ("drivewealth_user_id") REFERENCES "app"."drivewealth_users" ("ref_id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("trading_account_id") REFERENCES "app"."managed_portfolio_trading_accounts" ("id") ON UPDATE set null ON DELETE set null
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
CREATE TRIGGER "set_app_drivewealth_accounts_updated_at"
    BEFORE UPDATE
    ON "app"."drivewealth_accounts"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_drivewealth_accounts_updated_at" ON "app"."drivewealth_accounts"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';

