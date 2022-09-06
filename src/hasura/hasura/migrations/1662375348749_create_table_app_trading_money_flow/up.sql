CREATE TABLE "app"."trading_money_flow"
(
    "id"                 serial      NOT NULL,
    "profile_id"         int         not null,
    "trading_account_id" int         not null,
    "funding_account_id" int         not null,
    "status"             varchar,
    "amount"       numeric         not null,
    "created_at"         timestamptz NOT NULL DEFAULT now(),
    "updated_at"         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("trading_account_id") REFERENCES "app"."trading_accounts" ("id") ON UPDATE restrict ON DELETE restrict,
    FOREIGN KEY ("funding_account_id") REFERENCES "app"."trading_funding_accounts" ("id") ON UPDATE restrict ON DELETE restrict
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
CREATE TRIGGER "set_app_trading_money_flow_updated_at"
    BEFORE UPDATE
    ON "app"."trading_money_flow"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_trading_money_flow_updated_at" ON "app"."trading_money_flow"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';
