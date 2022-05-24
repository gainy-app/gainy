alter table "app"."notifications" add column "emails" jsonb
null default jsonb_build_array();

update "app"."notifications"
set emails = jsonb_build_array(profiles.email)
from "app"."profiles"
where profiles.id = notifications.profile_id;

alter table "app"."notifications" drop column "profile_id" cascade;
