CREATE TABLE "app"."queue_messages"
(
    "ref_id"              varchar                 NOT NULL,
    "source_ref_id"       varchar                 NOT NULL,
    "source_event_ref_id" varchar                 NOT NULL,
    "body"                json,
    "data"                json,
    "handled"             bool,
    "created_at"          timestamp default now() not null,
    "updated_at"          timestamp default now() not null,
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
CREATE TRIGGER "set_app_queue_messages_updated_at"
    BEFORE UPDATE
    ON "app"."queue_messages"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_queue_messages_updated_at" ON "app"."queue_messages"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';
