alter table "app"."subscriptions" drop constraint "subscriptions_invitation_id_key";
alter table "app"."subscriptions" add constraint "subscriptions_profile_id_invitation_id_key" unique ("profile_id", "invitation_id");
