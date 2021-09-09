alter table "app"."profiles" alter column "investment_experience" drop not null;
alter table "app"."profiles" add column "investment_experience" varchar;
