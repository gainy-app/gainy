alter table "app"."drivewealth_funds"
    alter column collection_id drop not null,
    alter column trading_collection_version_id drop not null,
    add column "symbol"           varchar null,
    add column "trading_order_id" int     null
        references app.trading_orders
            on update set null on delete set null;
