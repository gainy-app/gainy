CREATE VIEW "app"."analytics_subscriptions" AS
select id,
       profile_id,
       invitation_id,
       is_promotion,
       created_at::timestamp,
       extract(epoch FROM "period") as period,
       revenuecat_ref_id,
       revenuecat_entitlement_data::varchar
from app.subscriptions;
