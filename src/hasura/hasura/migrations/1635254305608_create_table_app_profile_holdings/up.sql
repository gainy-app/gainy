CREATE TABLE "app"."profile_holdings"
(
    "id"                serial      NOT NULL,
    "iso_currency_code" varchar     NOT NULL,
    "quantity"          float8      NOT NULL,
    "security_id"       integer     NOT NULL,
    "profile_id"        integer     NOT NULL,
    "account_id"                integer,
    "ref_id"            varchar     not null unique,
    "created_at"        timestamptz not null default now(),
    "updated_at"        timestamptz not null default now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("security_id") REFERENCES "app"."portfolio_securities" ("id") ON UPDATE cascade ON DELETE cascade,
    constraint "profile_holdings_account_id_fkey"
        foreign key ("account_id")
            references "app"."profile_portfolio_accounts"
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
CREATE TRIGGER "set_app_profile_holdings_updated_at"
    BEFORE UPDATE
    ON "app"."profile_holdings"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_profile_holdings_updated_at" ON "app"."profile_holdings"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';
