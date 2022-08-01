DROP VIEW "app"."analytics_profiles";
CREATE OR REPLACE VIEW "app"."analytics_profiles" AS
SELECT profiles.id,
       profiles.gender,
       profiles.created_at::timestamp,
       profiles.user_id,
       email ilike '%gainy.app'
           or email ilike '%test%'
           or last_name ilike '%test%'
           or first_name ilike '%test%' as is_test
FROM app.profiles;
