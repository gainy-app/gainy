alter table app.trading_collection_versions
    add column pending_execution_since timestamptz;
