begin transaction;
delete
from app.profile_plaid_access_tokens
where id in (
                select distinct profile_plaid_access_tokens.id
                from app.profile_plaid_access_tokens
                         join app.profile_plaid_access_tokens ppat2 using (item_id)
                where ppat2.id < profile_plaid_access_tokens.id
            );
alter table "app"."profile_plaid_access_tokens"
    add constraint "profile_plaid_access_tokens_item_id_key" unique ("item_id");
commit;
