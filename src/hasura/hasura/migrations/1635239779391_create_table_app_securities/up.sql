CREATE TABLE "app"."portfolio_securities"
(
    "id"                serial      NOT NULL,
    "close_price"       float8      NOT NULL,
    "close_price_as_of" date        NOT NULL,
    "iso_currency_code" varchar     NOT NULL,
    "name"              varchar     NOT NULL,
    "ref_id"            varchar     NOT NULL,
    "ticker_symbol"     varchar,
    "type"              varchar,
    "profile_id"        integer     NOT NULL,
    "created_at"        timestamptz NOT NULL DEFAULT now(),
    "updated_at"        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    UNIQUE ("ref_id"),
    constraint "portfolio_securities_profile_id_fkey"
        foreign key ("profile_id")
            references "app"."profiles"
                ("id") on update cascade on delete cascade
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
CREATE TRIGGER "set_app_portfolio_securities_updated_at"
    BEFORE UPDATE
    ON "app"."portfolio_securities"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_portfolio_securities_updated_at" ON "app"."portfolio_securities"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';
