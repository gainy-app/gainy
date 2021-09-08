alter table "app"."profile_scoring_settings" drop constraint "average_market_return";
alter table "app"."profile_scoring_settings" add constraint "average_market_return" check (CHECK (average_market_return = ANY (ARRAY[5, 15, 25, 50])));
