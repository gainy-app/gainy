alter table "app"."drivewealth_portfolios"
    add column "waiting_rebalance_since" timestamp,
    add column "last_rebalance_at" timestamp;
