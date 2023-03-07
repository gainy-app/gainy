alter table app.drivewealth_orders
    drop column "symbol_normalized";

drop index app.drivewealth_orders_account_id_symbol_normalized_date_idx;
