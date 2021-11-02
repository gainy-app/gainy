CREATE TABLE "app"."profile_portfolio_transactions"
(
    "id"                    serial      NOT NULL,
    "amount"                float8      NOT NULL,
    "date"                  date        NOT NULL,
    "fees"                  float8      NOT NULL,
    "iso_currency_code"     varchar     NOT NULL,
    "name"                  varchar     NOT NULL,
    "price"                 float8      NOT NULL,
    "quantity"              float8      NOT NULL,
    "subtype"               varchar     NOT NULL,
    "type"                  varchar     NOT NULL,
    "ref_id"                varchar     NOT NULL,
    "security_id"           integer     NOT NULL,
    "profile_id"            integer     NOT NULL,
    "account_id"            integer     NOT NULL,
    "created_at"            timestamptz NOT NULL DEFAULT now(),
    "updated_at"            timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("account_id") REFERENCES "app"."profile_portfolio_accounts" ("id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("security_id") REFERENCES "app"."portfolio_securities" ("id") ON UPDATE cascade ON DELETE cascade,
    UNIQUE ("ref_id")
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
CREATE TRIGGER "set_app_profile_portfolio_transactions_updated_at"
    BEFORE UPDATE
    ON "app"."profile_portfolio_transactions"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_profile_portfolio_transactions_updated_at" ON "app"."profile_portfolio_transactions"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';
