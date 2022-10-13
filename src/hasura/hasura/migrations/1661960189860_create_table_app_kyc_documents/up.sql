CREATE TABLE "app"."kyc_documents"
(
    "id"               serial      NOT NULL,
    "profile_id"       integer     NOT NULL,
    "uploaded_file_id" integer     NOT NULL unique,
    "content_type"     varchar     NOT NULL,
    "type"             varchar     NOT NULL,
    "side"             varchar     NOT NULL,
    "created_at"       timestamptz NOT NULL DEFAULT now(),
    "updated_at"       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("uploaded_file_id") REFERENCES "app"."uploaded_files" ("id") ON UPDATE cascade ON DELETE cascade,
    UNIQUE ("uploaded_file_id")
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
CREATE TRIGGER "set_app_kyc_documents_updated_at"
    BEFORE UPDATE
    ON "app"."kyc_documents"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_kyc_documents_updated_at" ON "app"."kyc_documents"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';
