CREATE TABLE "app"."verification_codes"
(
    "id"          serial      NOT NULL,
    "profile_id"  int         NOT NULL,
    "channel"     varchar     NOT NULL,
    "address"     varchar     NOT NULL,
    "code"        varchar,
    "failed_at"   timestamptz,
    "verified_at" timestamptz,
    "created_at"  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade
);
