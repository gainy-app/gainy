alter table "app"."profile_flags" alter column "is_trading_enabled" set default 'true';
update app.profile_flags set is_trading_enabled = true where not is_trading_enabled;

insert into app.profile_flags(profile_id)
select id from app.profiles;
