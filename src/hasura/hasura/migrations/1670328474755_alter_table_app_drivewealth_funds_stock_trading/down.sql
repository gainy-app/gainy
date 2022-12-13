alter table "app"."drivewealth_funds"
    alter column collection_id set not null,
    alter column trading_collection_version_id set not null,
    drop column "symbol",
    drop column "trading_order_id";
