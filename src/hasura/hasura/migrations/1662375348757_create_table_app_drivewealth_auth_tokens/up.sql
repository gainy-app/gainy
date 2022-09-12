CREATE TABLE "app"."drivewealth_auth_tokens"
(
    "id"         serial      NOT NULL,
    "auth_token" varchar,
    "expires_at" timestamptz,
    "version"    int         NOT NULL,
    "data"       json,
    "created_at" timestamptz NOT NULL DEFAULT now(),
    "updated_at" timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id")
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
CREATE TRIGGER "set_app_drivewealth_auth_tokens_updated_at"
    BEFORE UPDATE
    ON "app"."drivewealth_auth_tokens"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_drivewealth_auth_tokens_updated_at" ON "app"."drivewealth_auth_tokens"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';

