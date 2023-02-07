alter table app.trading_collection_versions
    add executed_amount numeric;
update app.trading_collection_versions set executed_amount = target_amount_delta where status = 'EXECUTED_FULLY';

alter table app.trading_orders
    add executed_amount numeric;
update app.trading_orders set executed_amount = target_amount_delta where status = 'EXECUTED_FULLY';
