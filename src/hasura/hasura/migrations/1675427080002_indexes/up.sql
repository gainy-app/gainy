create index trading_collection_versions_profile_id_collection_id_index
    on app.trading_collection_versions (profile_id, collection_id);
create index trading_orders_profile_id_symbol_index
    on app.trading_orders (profile_id, symbol);
create index drivewealth_portfolio_statuses_drivewealth_portfolio_id_date_created_at_index
    on app.drivewealth_portfolio_statuses (drivewealth_portfolio_id, date, created_at);
