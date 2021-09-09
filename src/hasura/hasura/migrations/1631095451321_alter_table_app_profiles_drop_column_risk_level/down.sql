alter table "app"."profiles" alter column "risk_level" drop not null;
alter table "app"."profiles" add column "risk_level" float4;
