CREATE TABLE "app"."drivewealth_instruments"
(
    "ref_id"     varchar     NOT NULL,
    "symbol"     varchar     NOT NULL,
    "status"     varchar     NOT NULL,
    "data"       json,
    "created_at" timestamptz NOT NULL DEFAULT now(),
    "updated_at" timestamptz NOT NULL DEFAULT now(),
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
CREATE TRIGGER "set_app_drivewealth_instruments_updated_at"
    BEFORE UPDATE
    ON "app"."drivewealth_instruments"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_drivewealth_instruments_updated_at" ON "app"."drivewealth_instruments"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';
