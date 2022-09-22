CREATE TABLE "app"."invoices"
(
    "id"           serial                  NOT NULL,
    "profile_id"   int,
    "period_id"    varchar,
    "status"       varchar   default 'PENDING',
    "amount"       numeric,
    "due_date"     date,
    "description"  text,
    "period_start" timestamp,
    "period_end"   timestamp,
    "metadata"     json,
    "version"      int,
    "created_at"   timestamp default now() not null,
    PRIMARY KEY ("id"),
    UNIQUE ("profile_id", "period_id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE restrict ON DELETE restrict
);
