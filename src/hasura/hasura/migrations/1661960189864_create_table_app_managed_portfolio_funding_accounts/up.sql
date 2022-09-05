CREATE TABLE "app"."trading_funding_accounts"
(
    "id"                    serial      NOT NULL,
    "profile_id"            integer     NOT NULL,
    "plaid_access_token_id" integer,
    "plaid_account_id"      varchar,
    "name"                  varchar,
    "balance"               double precision,
    "created_at"            timestamptz NOT NULL DEFAULT now(),
    "updated_at"            timestamptz NOT NULL DEFAULT now(),
    constraint "trading_funding_accounts_plaid_key" unique ("plaid_access_token_id", "plaid_account_id"),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("plaid_access_token_id") REFERENCES "app"."profile_plaid_access_tokens" ("id") ON UPDATE restrict ON DELETE restrict
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
CREATE TRIGGER "set_app_trading_funding_accounts_updated_at"
    BEFORE UPDATE
    ON "app"."trading_funding_accounts"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_trading_funding_accounts_updated_at" ON "app"."trading_funding_accounts"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';

