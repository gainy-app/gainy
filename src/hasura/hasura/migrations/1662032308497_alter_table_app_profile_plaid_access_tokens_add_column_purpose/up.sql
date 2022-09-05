alter table "app"."profile_plaid_access_tokens" add column "purpose" varchar
 not null default 'portfolio';
