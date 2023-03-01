begin transaction;

alter table app.drivewealth_orders
    add column "symbol_normalized" varchar;

update app.drivewealth_orders
set symbol_normalized = normalize_drivewealth_symbol(symbol)
where symbol_normalized is null;

alter table app.drivewealth_orders
    alter column "symbol_normalized" set not null;

commit;

create index on app.drivewealth_orders (account_id, symbol_normalized, date);
