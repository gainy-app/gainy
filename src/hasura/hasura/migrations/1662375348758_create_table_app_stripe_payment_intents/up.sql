CREATE TABLE "app"."stripe_payment_intents"
(
    "ref_id"                       varchar unique,
    "status"                       varchar,
    "authentication_client_secret" varchar,
    "to_refund"                    bool,
    "is_refunded"                  bool,
    "data"                         json,
    "refund_data"                  json,
    "created_at"                   timestamptz NOT NULL DEFAULT now(),
    "updated_at"                   timestamptz NOT NULL DEFAULT now(),
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
CREATE TRIGGER "set_app_stripe_payment_intents_updated_at"
    BEFORE UPDATE
    ON "app"."stripe_payment_intents"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_stripe_payment_intents_updated_at" ON "app"."stripe_payment_intents"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';

