CREATE TABLE "app"."kyc_statuses"
(
    "id"             serial      NOT NULL,
    "profile_id"     int         NOT NULL,
    "status"         varchar     NOT NULL,
    "message"        varchar     NOT NULL,
    "error_messages" json,
    "created_at"     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade
);
