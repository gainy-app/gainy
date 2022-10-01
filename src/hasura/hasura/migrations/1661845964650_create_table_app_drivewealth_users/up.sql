CREATE TABLE "app"."drivewealth_users"
(
    "ref_id"     varchar NOT NULL,
    "profile_id" integer          unique,
    "status"     varchar NOT NULL,
    "data"       json    NOT NULL,
    "created_at"  timestamptz NOT NULL DEFAULT now(),
    "updated_at"  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("ref_id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE restrict ON DELETE restrict
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
CREATE TRIGGER "set_app_drivewealth_users_updated_at"
    BEFORE UPDATE
    ON "app"."drivewealth_users"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_drivewealth_users_updated_at" ON "app"."drivewealth_users"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';

