CREATE TABLE "app"."profile_ticker_metrics_settings"
(
    "id"            serial      NOT NULL,
    "profile_id"    integer     NOT NULL,
    "collection_id" integer,
    "field_name"    varchar     NOT NULL,
    "order"         integer     NOT NULL,
    "created_at"    timestamptz NOT NULL DEFAULT now(),
    "updated_at"    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade,
    CONSTRAINT "field_name" CHECK (field_name in (
                                                  'market_capitalization',
                                                  'month_price_change',
                                                  'quarterly_revenue_growth_yoy',
                                                  'enterprise_value_to_sales',
                                                  'profit_margin',
                                                  'avg_volume_10d',
                                                  'short_percent_outstanding',
                                                  'shares_outstanding',
                                                  'avg_volume_90d',
                                                  'shares_float',
                                                  'short_ratio',
                                                  'beta'
        ))
);

CREATE UNIQUE INDEX profile_id__collection_id__field_name__key ON "app"."profile_ticker_metrics_settings" (profile_id, collection_id, field_name) WHERE collection_id IS NOT NULL;
CREATE UNIQUE INDEX profile_id__field_name__key ON "app"."profile_ticker_metrics_settings" (profile_id, field_name) WHERE collection_id IS NULL;

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
CREATE TRIGGER "set_app_profile_ticker_metrics_settings_updated_at"
    BEFORE UPDATE
    ON "app"."profile_ticker_metrics_settings"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_profile_ticker_metrics_settings_updated_at" ON "app"."profile_ticker_metrics_settings"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';
