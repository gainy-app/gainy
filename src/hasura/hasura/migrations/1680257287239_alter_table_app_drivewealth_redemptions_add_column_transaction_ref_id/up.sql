alter table "app"."drivewealth_redemptions"
    add column "transaction_ref_id" varchar null;

update "app"."drivewealth_redemptions"
set transaction_ref_id = data ->> 'finTranRef'
where transaction_ref_id is null;
