CREATE TABLE "app"."drivewealth_portfolio_statuses"
(
    "id"                       serial      NOT NULL,
    "drivewealth_portfolio_id" varchar     NOT NULL,
    "data"                     json,
    "created_at"               timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("drivewealth_portfolio_id") REFERENCES "app"."drivewealth_portfolios" ("ref_id") ON UPDATE cascade ON DELETE cascade
);
