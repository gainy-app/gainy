alter table "app"."drivewealth_portfolios"
    alter column "waiting_rebalance_since" type timestamp,
    alter column "last_rebalance_at" type timestamp;

