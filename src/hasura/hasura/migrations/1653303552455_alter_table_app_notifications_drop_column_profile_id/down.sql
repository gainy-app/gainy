alter table "app"."notifications"
  add constraint "notifications_profile_id_fkey"
  foreign key (profile_id)
  references "app"."profiles"
  (id) on update cascade on delete cascade;
alter table "app"."notifications" alter column "profile_id" drop not null;
alter table "app"."notifications" add column "profile_id" int4;

alter table "app"."notifications" drop column "emails" cascade;
