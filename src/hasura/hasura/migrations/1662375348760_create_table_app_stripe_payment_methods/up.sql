CREATE TABLE "app"."stripe_payment_methods"
(
    "ref_id"            varchar     NOT NULL,
    "payment_method_id" int unique,
    "name"              varchar not null,
    "data"              json,
    "created_at"        timestamptz NOT NULL DEFAULT now(),
    "updated_at"        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("ref_id"),
    FOREIGN KEY ("payment_method_id") REFERENCES "app"."payment_methods" ("id") ON UPDATE cascade ON DELETE cascade
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
CREATE TRIGGER "set_app_stripe_payment_methods_updated_at"
    BEFORE UPDATE
    ON "app"."stripe_payment_methods"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_stripe_payment_methods_updated_at" ON "app"."stripe_payment_methods"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';

