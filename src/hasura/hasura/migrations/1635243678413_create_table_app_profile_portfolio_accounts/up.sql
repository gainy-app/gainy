CREATE TABLE "app"."profile_portfolio_accounts"
(
    "id"                        serial           NOT NULL,
    "ref_id"                    varchar          NOT NULL,
    "balance_available"         double precision,
    "balance_current"           double precision NOT NULL,
    "balance_iso_currency_code" varchar          NOT NULL,
    "balance_limit"             double precision,
    "mask"                      varchar          NOT NULL,
    "name"                      varchar          NOT NULL,
    "official_name"             varchar,
    "subtype"                   varchar          NOT NULL,
    "type"                      varchar          NOT NULL,
    "profile_id"                integer          NOT NULL,
    "created_at"                timestamptz      NOT NULL DEFAULT now(),
    "updated_at"                timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade,
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
CREATE TRIGGER "set_app_profile_portfolio_accounts_updated_at"
    BEFORE UPDATE
    ON "app"."profile_portfolio_accounts"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_profile_portfolio_accounts_updated_at" ON "app"."profile_portfolio_accounts"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';
