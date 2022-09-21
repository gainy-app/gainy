CREATE TABLE "app"."payment_transactions"
(
    "id"                serial                  NOT NULL,
    "profile_id"        int                     not null,
    "invoice_id"        int                     not null,
    "payment_method_id" int                     not null,
    "status"            varchar                 not null,
    "metadata"          json,
    "created_at"        timestamp default now() not null,
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE restrict ON DELETE restrict,
    FOREIGN KEY ("invoice_id") REFERENCES "app"."invoices" ("id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("payment_method_id") REFERENCES "app"."payment_methods" ("id") ON UPDATE set null ON DELETE set null
);