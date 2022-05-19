CREATE TABLE "app"."notifications"
(
    "uuid"       uuid        NOT NULL DEFAULT gen_random_uuid(),
    "profile_id" integer,
    "uniq_id"    varchar     NOT NULL,
    "send_at"    timestamptz,
    "text"       jsonb       NOT NULL,
    "data"       jsonb,
    "sender_id"  uuid,
    "response"   jsonb,
    "created_at" timestamptz null     default now(),
    "updated_at" timestamptz null     default now(),
    PRIMARY KEY ("uuid"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade,
    UNIQUE ("uniq_id")
);
CREATE EXTENSION IF NOT EXISTS pgcrypto;


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
CREATE TRIGGER "set_app_notifications_updated_at"
    BEFORE UPDATE
    ON "app"."notifications"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_notifications_updated_at" ON "app"."notifications"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';
