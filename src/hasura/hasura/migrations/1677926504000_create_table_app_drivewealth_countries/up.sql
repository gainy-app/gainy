create table app.drivewealth_countries
(
    ref_id     varchar     not null,
    code2      varchar     not null,
    code3      varchar     not null,
    name       varchar     not null,
    active     bool,
    data       json,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    PRIMARY KEY ("ref_id")
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
CREATE TRIGGER "set_app_drivewealth_countries_updated_at"
    BEFORE UPDATE
    ON "app"."drivewealth_countries"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_drivewealth_countries_updated_at" ON "app"."drivewealth_countries"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';
