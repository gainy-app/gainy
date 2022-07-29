alter table "app"."subscriptions"
    add column "ref_id"       varchar   null,
    add column "product_id"   varchar   null,
    add column "promocode_id" int       null,
    add column "tariff"       varchar   null,
    add column "expired_at"   timestamp null,
    add constraint "subscriptions_profile_id_ref_id_key" unique ("profile_id", "ref_id");

update app.subscriptions
set ref_id = (revenuecat_entitlement_data->>'product_identifier') || '_' || (revenuecat_entitlement_data->>'purchase_date'),
    product_id = revenuecat_ref_id,
    tariff = revenuecat_entitlement_data->>'product_identifier';
