CREATE TABLE "app"."trading_statements"
(
    "id"           serial      NOT NULL,
    "profile_id"   int         NOT NULL,
    "type"         varchar     NOT NULL,
    "display_name" varchar     NOT NULL,
    "created_at"   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id")
);
