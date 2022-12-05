CREATE TABLE "app"."profile_flags"
(
    "profile_id"                 integer NOT NULL,
    "is_region_changing_allowed" boolean NOT NULL default false,
    "is_trading_enabled"         boolean NOT NULL default false,
    PRIMARY KEY ("profile_id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade
);
