CREATE TABLE "app"."verification_codes"
(
    "id"          uuid        NOT NULL DEFAULT gen_random_uuid(),
    "profile_id"  int         NOT NULL,
    "channel"     varchar     NOT NULL,
    "address"     varchar     NOT NULL,
    "failed_at"   timestamptz,
    "verified_at" timestamptz,
    "created_at"  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade
);
