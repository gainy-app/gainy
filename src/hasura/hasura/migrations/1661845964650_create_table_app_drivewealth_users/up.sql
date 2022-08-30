CREATE TABLE "app"."drivewealth_users"
(
    "ref_id"     varchar NOT NULL,
    "profile_id" integer NOT NULL unique,
    "status"     varchar NOT NULL,
    "data"       json    NOT NULL,
    PRIMARY KEY ("ref_id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade
);
