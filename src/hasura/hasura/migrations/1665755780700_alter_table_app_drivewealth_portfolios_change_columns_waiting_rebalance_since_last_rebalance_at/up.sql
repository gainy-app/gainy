alter table "app"."drivewealth_portfolios"
    alter column "waiting_rebalance_since" type timestamptz,
    alter column "last_rebalance_at" type timestamptz;

