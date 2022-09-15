CREATE TABLE "app"."drivewealth_accounts_money"
(
    "id"                            serial      NOT NULL,
    "drivewealth_account_id"        varchar     NOT NULL,
    "cash_available_for_trade"      numeric,
    "cash_available_for_withdrawal" numeric,
    "cash_balance"                  numeric,
    "data"                          json,
    "created_at"                    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("drivewealth_account_id") REFERENCES "app"."drivewealth_accounts" ("ref_id") ON UPDATE restrict ON DELETE restrict
);
