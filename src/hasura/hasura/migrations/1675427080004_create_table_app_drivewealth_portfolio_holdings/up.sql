create table app.drivewealth_portfolio_holdings
(
    holding_id_v2       varchar     not null,
    profile_id          int         not null,
    portfolio_status_id int         not null,
    actual_value        numeric,
    quantity            numeric,
    symbol              varchar     not null,
    collection_uniq_id  varchar,
    collection_id       int,
    updated_at          timestamptz not null default now(),
    PRIMARY KEY ("holding_id_v2"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("portfolio_status_id") REFERENCES "app"."drivewealth_portfolio_statuses" ("id") ON UPDATE cascade ON DELETE cascade
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
CREATE TRIGGER "set_app_drivewealth_portfolio_holdings_updated_at"
    BEFORE UPDATE
    ON "app"."drivewealth_portfolio_holdings"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_drivewealth_portfolio_holdings_updated_at" ON "app"."drivewealth_portfolio_holdings"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';
