alter table "app"."profile_interests"
  add constraint "profile_interests_interest_id_fkey"
  foreign key ("interest_id")
  references "app"."interests"
  ("id") on update cascade on delete cascade;
