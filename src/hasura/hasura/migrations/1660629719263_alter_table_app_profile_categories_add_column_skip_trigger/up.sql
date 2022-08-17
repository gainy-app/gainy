alter table "app"."profile_categories" add column "skip_trigger" boolean
 not null default 'false';
