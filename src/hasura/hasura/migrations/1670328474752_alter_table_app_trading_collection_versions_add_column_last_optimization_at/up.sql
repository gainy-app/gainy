alter table "app"."trading_collection_versions"
    add column "source"               varchar null,
    add column "last_optimization_at" date    null;

update app.trading_collection_versions
set source = 'MANUAL'
where source is null;
