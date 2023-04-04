alter table app.trading_statements
    add column date date;

create index on app.trading_statements (profile_id, date);
