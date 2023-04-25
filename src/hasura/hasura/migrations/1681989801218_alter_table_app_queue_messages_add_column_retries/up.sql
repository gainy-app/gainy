alter table "app"."queue_messages" add column "retries" integer
 not null default '0';
