alter table app.drivewealth_redemptions
    add column "payment_transaction_id" int,
    add foreign key ("payment_transaction_id") REFERENCES "app"."payment_transactions" ("id") ON UPDATE cascade ON DELETE set null;
