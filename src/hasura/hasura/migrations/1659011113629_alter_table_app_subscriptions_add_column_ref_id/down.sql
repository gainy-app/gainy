alter table "app"."subscriptions"
    drop column "ref_id",
    drop column "product_id",
    drop column "promocode_id",
    drop column "tariff",
    drop column "expired_at",
    drop constraint "subscriptions_profile_id_ref_id_key";
