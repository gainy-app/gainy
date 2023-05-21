alter table "app"."profile_flags"
    add column trading_paused bool not null default false;
