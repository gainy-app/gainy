create index trading_collection_versions_profile_id_collection_id_index
    on app.trading_collection_versions (profile_id, collection_id);
create index trading_orders_profile_id_symbol_index
    on app.trading_orders (profile_id, symbol);
