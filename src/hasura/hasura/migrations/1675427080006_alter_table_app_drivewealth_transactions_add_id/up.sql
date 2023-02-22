create unique index on app.drivewealth_transactions (ref_id);

alter table app.drivewealth_transactions
    drop constraint drivewealth_transactions_pkey,
    add column "id" serial primary key not null;
