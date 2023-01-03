alter table "app"."trading_collection_versions"
    alter column "target_amount_delta" set not null,
    drop column "target_amount_delta_relative";
alter table "app"."trading_orders"
    alter column "target_amount_delta" set not null,
    drop column "target_amount_delta_relative";
