CREATE TABLE "app"."managed_portfolio_funding_accounts"
(
    "id"                    serial      NOT NULL,
    "profile_id"            integer     NOT NULL,
    "plaid_access_token_id" integer unique ,
    "name"                  varchar,
--     "balance"      integer,
    "created_at"            timestamptz NOT NULL DEFAULT now(),
    "updated_at"            timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("plaid_access_token_id") REFERENCES "app"."profile_plaid_access_tokens" ("id") ON UPDATE cascade ON DELETE cascade
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
CREATE TRIGGER "set_app_managed_portfolio_funding_accounts_updated_at"
    BEFORE UPDATE
    ON "app"."managed_portfolio_funding_accounts"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_managed_portfolio_funding_accounts_updated_at" ON "app"."managed_portfolio_funding_accounts"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';

