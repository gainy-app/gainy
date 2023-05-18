create table "app"."corporate_action_adjustments"
(
    id                 serial primary key,
    profile_id         int                                    not null,
    trading_account_id int                                    not null,
    collection_id      int,
    symbol             text,
    amount             numeric                                not null,
    created_at         timestamp with time zone default now() not null,
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("trading_account_id") REFERENCES "app"."trading_accounts" ("id") ON UPDATE cascade ON DELETE cascade
);

create table "app"."corporate_action_drivewealth_transaction_link"
(
    corporate_action_adjustment_id int                                    not null,
    drivewealth_transaction_id     int                                    not null,
    created_at                     timestamp with time zone default now() not null,
    primary key (corporate_action_adjustment_id, drivewealth_transaction_id),
    FOREIGN KEY ("corporate_action_adjustment_id") REFERENCES "app"."corporate_action_adjustments" ("id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("drivewealth_transaction_id") REFERENCES "app"."drivewealth_transactions" ("id") ON UPDATE cascade ON DELETE cascade
);

alter table "app"."trading_orders"
    add note text;

alter table "app"."trading_collection_versions"
    add note text;