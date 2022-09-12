CREATE TABLE "app"."payment_methods"
(
    "id"            serial      NOT NULL,
    "profile_id"    int         not null,
    "name"          varchar     not null,
    "set_active_at" timestamp,
    "created_at"    timestamptz NOT NULL DEFAULT now(),
    "updated_at"    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade
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
CREATE TRIGGER "set_app_payment_methods_updated_at"
    BEFORE UPDATE
    ON "app"."payment_methods"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_payment_methods_updated_at" ON "app"."payment_methods"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';
