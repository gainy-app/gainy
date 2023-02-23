alter table app.drivewealth_transactions
    drop column id,
    add primary key (ref_id);

drop index app.drivewealth_transactions_ref_id_idx;
