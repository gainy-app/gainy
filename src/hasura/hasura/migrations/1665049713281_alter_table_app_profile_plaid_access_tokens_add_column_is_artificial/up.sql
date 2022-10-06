alter table "app"."profile_plaid_access_tokens" add column "is_artificial" boolean
 not null default 'false';
