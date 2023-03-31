alter table "app"."drivewealth_redemptions"
    add column "transaction_ref_id" varchar null;

update "app"."drivewealth_redemptions"
set transaction_ref_id = coalesce(data ->> 'finTranRef', data ->> 'finTranReference')
where transaction_ref_id is null;
