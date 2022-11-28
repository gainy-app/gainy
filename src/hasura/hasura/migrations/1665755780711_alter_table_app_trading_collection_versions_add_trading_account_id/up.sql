alter table app.trading_collection_versions
    add column trading_account_id int,
    add FOREIGN KEY ("trading_account_id") REFERENCES "app"."trading_accounts" ("id") ON UPDATE cascade ON DELETE cascade;

with profile_trading_accounts as
    (
        select distinct on (profile_id) profile_id, id
        from app.trading_accounts
    )
update app.trading_collection_versions
    set trading_account_id = profile_trading_accounts.id
from profile_trading_accounts
where profile_trading_accounts.profile_id = trading_collection_versions.profile_id
  and trading_account_id is null;

alter table app.trading_collection_versions
    alter column trading_account_id set not null;
