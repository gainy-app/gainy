alter table "app"."trading_collection_versions"
    alter column "target_amount_delta" drop not null,
    add column "target_amount_delta_relative" numeric null;
alter table "app"."trading_orders"
    alter column "target_amount_delta" drop not null,
    add column "target_amount_delta_relative" numeric null;
