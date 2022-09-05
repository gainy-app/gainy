CREATE TABLE "app"."trading_accounts"
(
    "id"                            serial      NOT NULL,
    "profile_id"                    integer     NOT NULL unique,
    "name"                          varchar,
    "cash_available_for_trade"      integer,
    "cash_available_for_withdrawal" integer,
    "cash_balance"                  integer,
    "created_at"                    timestamptz NOT NULL DEFAULT now(),
    "updated_at"                    timestamptz NOT NULL DEFAULT now(),
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
CREATE TRIGGER "set_app_trading_accounts_updated_at"
    BEFORE UPDATE
    ON "app"."trading_accounts"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_trading_accounts_updated_at" ON "app"."trading_accounts"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';

