alter table "app"."portfolio_securities"
    add column "profile_id" integer NULL,
    add constraint "portfolio_securities_profile_id_fkey"
        foreign key ("profile_id")
            references "app"."profiles"
                ("id") on update cascade on delete cascade;
