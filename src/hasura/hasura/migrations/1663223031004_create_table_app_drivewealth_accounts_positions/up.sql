CREATE TABLE "app"."drivewealth_accounts_positions"
(
    "id"                     serial      NOT NULL,
    "drivewealth_account_id" varchar     NOT NULL,
    "equity_value"           numeric,
    "data"                   json,
    "created_at"             timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("drivewealth_account_id") REFERENCES "app"."drivewealth_accounts" ("ref_id") ON UPDATE cascade ON DELETE cascade
);
