CREATE VIEW "app"."analytics_subscriptions" AS
select id,
       profile_id,
       invitation_id,
       is_promotion,
       created_at::timestamp,
       extract(epoch FROM "period") as period,
       ref_id
       product_id,
       promocode_id,
       tariff,
       expired_at,
       revenuecat_entitlement_data::varchar
from app.subscriptions;
