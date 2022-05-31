CREATE TABLE "app"."invitations"
(
    "id"              serial      NOT NULL,
    "from_profile_id" integer     NOT NULL,
    "to_profile_id"   integer     NOT NULL,
    "created_at"      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("from_profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("to_profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade,
    UNIQUE ("to_profile_id")
);
