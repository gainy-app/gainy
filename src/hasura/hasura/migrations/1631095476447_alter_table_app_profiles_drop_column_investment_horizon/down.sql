alter table "app"."profiles" alter column "investment_horizon" drop not null;
alter table "app"."profiles" add column "investment_horizon" float4;
