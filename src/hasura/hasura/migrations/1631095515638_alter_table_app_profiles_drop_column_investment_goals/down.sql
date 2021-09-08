alter table "app"."profiles" alter column "investment_goals" drop not null;
alter table "app"."profiles" add column "investment_goals" jsonb;
