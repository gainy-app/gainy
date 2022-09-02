CREATE TABLE "app"."drivewealth_documents"
(
    "ref_id"          varchar     NOT NULL,
    "kyc_document_id" integer,
    "status"          varchar     NOT NULL,
    "data"            json        NOT NULL,
    "created_at"      timestamptz NOT NULL DEFAULT now(),
    "updated_at"      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("ref_id"),
    FOREIGN KEY ("kyc_document_id") REFERENCES "app"."kyc_documents" ("id") ON UPDATE set null ON DELETE set null
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
CREATE TRIGGER "set_app_drivewealth_documents_updated_at"
    BEFORE UPDATE
    ON "app"."drivewealth_documents"
    FOR EACH ROW
EXECUTE PROCEDURE "app"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_app_drivewealth_documents_updated_at" ON "app"."drivewealth_documents"
    IS 'trigger to set value of column "updated_at" to current timestamp on row update';

