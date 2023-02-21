create table app.drivewealth_transactions
(
    ref_id               varchar                                not null
        primary key,
    account_id           varchar                                not null,
    type                 varchar                                not null,
    symbol               varchar,
    account_amount_delta numeric,
    datetime             timestamptz,
    date                 date,
    data                 json,
    created_at           timestamp with time zone default now() not null
);

create index on app.drivewealth_transactions (account_id, type);
