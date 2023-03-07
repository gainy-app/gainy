drop index app.trading_statements_profile_id_date_idx;
alter table app.trading_statements
    drop column date;
