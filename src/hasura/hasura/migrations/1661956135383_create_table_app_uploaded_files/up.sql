CREATE TABLE "app"."uploaded_files"
(
    "id"           serial      NOT NULL,
    "profile_id"   integer     NOT NULL,
    "s3_bucket"    varchar     NOT NULL,
    "s3_key"       varchar     NOT NULL,
    "content_type" varchar     NOT NULL,
    "created_at"   timestamptz NOT NULL DEFAULT now(),
    "updated_at"   timestamptz NOT NULL DEFAULT now(),
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
CREATE TRIGGER "set_app_uploaded_files_updated_at"
    BEFORE UPDATE
    ON "app"."uploaded_files"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_uploaded_files_updated_at" ON "app"."uploaded_files"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';
