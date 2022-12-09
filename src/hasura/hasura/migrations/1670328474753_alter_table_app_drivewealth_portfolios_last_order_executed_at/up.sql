alter table "app"."drivewealth_portfolios"
    add column "last_order_executed_at" timestamptz null,
    add column "last_sync_at" timestamptz null;
