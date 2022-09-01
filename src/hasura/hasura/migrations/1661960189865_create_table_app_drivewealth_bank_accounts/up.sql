CREATE TABLE "app"."drivewealth_bank_accounts"
(
    "ref_id"                               varchar     NOT NULL,
    "drivewealth_user_id"                  varchar     NOT NULL,
    "managed_portfolio_funding_account_id" integer unique,
    "plaid_access_token_id" integer unique,
    "status"                               varchar,
    "bank_account_nickname"                varchar,
    "bank_account_number"                  varchar,
    "bank_routing_number"                  varchar,
    "data"                                 json,
    "created_at"                           timestamptz NOT NULL DEFAULT now(),
    "updated_at"                           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("ref_id"),
    FOREIGN KEY ("drivewealth_user_id") REFERENCES "app"."drivewealth_users" ("ref_id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("plaid_access_token_id") REFERENCES "app"."profile_plaid_access_tokens" ("id") ON UPDATE set null ON DELETE set null,
    FOREIGN KEY ("managed_portfolio_funding_account_id") REFERENCES "app"."managed_portfolio_funding_accounts" ("id") ON UPDATE set null ON DELETE set null
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
CREATE TRIGGER "set_app_drivewealth_bank_accounts_updated_at"
    BEFORE UPDATE
    ON "app"."drivewealth_bank_accounts"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_drivewealth_bank_accounts_updated_at" ON "app"."drivewealth_bank_accounts"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';

