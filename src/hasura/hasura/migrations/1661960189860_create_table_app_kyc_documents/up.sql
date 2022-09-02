CREATE TABLE "app"."kyc_documents"
(
    "id"               serial      NOT NULL,
    "profile_id"       integer     NOT NULL,
    "uploaded_file_id" integer     NOT NULL unique,
    "content_type"     varchar     NOT NULL,
    "type"             varchar     NOT NULL,
    "side"             varchar     NOT NULL,
    "created_at"       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("uploaded_file_id") REFERENCES "app"."uploaded_files" ("id") ON UPDATE cascade ON DELETE cascade,
    UNIQUE ("uploaded_file_id")
);
